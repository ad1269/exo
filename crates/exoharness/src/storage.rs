use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::fs;

use crate::Result;

#[derive(Clone)]
pub(crate) struct BasicObjectStore {
    store: Arc<dyn ObjectStore>,
    root: PathBuf,
}

impl BasicObjectStore {
    pub(crate) async fn local_filesystem(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root).await?;
        let store = LocalFileSystem::new_with_prefix(&root)?;
        Ok(Self {
            store: Arc::new(store),
            root,
        })
    }

    /// `put_json` that survives a crash: the object store's local put stages
    /// and renames but never syncs, so this writes directly — temp file,
    /// `sync_all` (F_FULLFSYNC on macOS), rename. The rename itself is on
    /// disk only after `sync_dir` on the parent; batch writers call that
    /// once per batch rather than per file.
    pub(crate) async fn put_json_fsync<T: Serialize>(
        &self,
        key: impl AsRef<Path>,
        value: &T,
    ) -> Result<()> {
        let path = self.root.join(normalize_path(key.as_ref()));
        let bytes = serde_json::to_vec_pretty(value)?;
        tokio::task::spawn_blocking(move || write_fsync(&path, &bytes)).await?
    }

    pub(crate) async fn sync_dir(&self, key: impl AsRef<Path>) -> Result<()> {
        let path = self.root.join(normalize_path(key.as_ref()));
        tokio::task::spawn_blocking(move || {
            std::fs::File::open(&path)?
                .sync_all()
                .with_context(|| format!("failed to sync directory {}", path.display()))
        })
        .await?
    }

    pub(crate) async fn put_json<T: Serialize>(
        &self,
        key: impl AsRef<Path>,
        value: &T,
    ) -> Result<()> {
        let path = object_path(key.as_ref())?;
        let bytes = serde_json::to_vec_pretty(value)?;
        self.store.put(&path, Bytes::from(bytes).into()).await?;
        Ok(())
    }

    pub(crate) async fn put_bytes(&self, key: impl AsRef<Path>, value: Vec<u8>) -> Result<()> {
        let path = object_path(key.as_ref())?;
        self.store.put(&path, Bytes::from(value).into()).await?;
        Ok(())
    }

    pub(crate) async fn get_json<T: DeserializeOwned>(&self, key: impl AsRef<Path>) -> Result<T> {
        let key = key.as_ref();
        let bytes = self.get_bytes(key).await?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to decode JSON {}", key.display()))
    }

    pub(crate) async fn get_json_if_exists<T: DeserializeOwned>(
        &self,
        key: impl AsRef<Path>,
    ) -> Result<Option<T>> {
        let key = key.as_ref();
        let Some(bytes) = self.get_bytes_if_exists(key).await? else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .with_context(|| format!("failed to decode JSON {}", key.display()))
    }

    pub(crate) async fn get_bytes(&self, key: impl AsRef<Path>) -> Result<Vec<u8>> {
        let key = key.as_ref();
        let path = object_path(key)?;
        let bytes = self
            .store
            .get(&path)
            .await
            .with_context(|| format!("failed to get {}", key.display()))?
            .bytes()
            .await?;
        Ok(bytes.to_vec())
    }

    pub(crate) async fn get_bytes_if_exists(
        &self,
        key: impl AsRef<Path>,
    ) -> Result<Option<Vec<u8>>> {
        let path = object_path(key.as_ref())?;
        let get_result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        Ok(Some(get_result.bytes().await?.to_vec()))
    }

    pub(crate) async fn list_keys(&self, prefix: impl AsRef<Path>) -> Result<Vec<String>> {
        let prefix = normalize_path(prefix.as_ref());
        let object_prefix = match prefix.is_empty() {
            true => None,
            false => Some(object_prefix(Path::new(&prefix))?),
        };
        let mut keys = self
            .store
            .list(object_prefix.as_ref())
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await?;
        keys.sort();
        Ok(keys)
    }

    /// Delete the object at exactly `key`, tolerating absence. Unlike
    /// `delete_prefix`, this works for a single object: `list`-based prefix
    /// deletion never matches an object at exactly the prefix path.
    pub(crate) async fn delete_key_if_exists(&self, key: impl AsRef<Path>) -> Result<()> {
        match self.store.delete(&object_path(key.as_ref())?).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) async fn delete_prefix(&self, prefix: impl AsRef<Path>) -> Result<()> {
        for key in self.list_keys(prefix).await? {
            self.store.delete(&object_path(Path::new(&key))?).await?;
        }
        Ok(())
    }

    pub(crate) async fn copy_prefix(
        &self,
        src_prefix: impl AsRef<Path>,
        dst_prefix: impl AsRef<Path>,
    ) -> Result<()> {
        let src_prefix = normalize_path(src_prefix.as_ref());
        let dst_prefix = normalize_path(dst_prefix.as_ref());
        for key in self.list_keys(&src_prefix).await? {
            let relative = key
                .strip_prefix(&src_prefix)
                .expect("listed key should share prefix");
            let destination = format!("{dst_prefix}{relative}");
            self.store
                .copy(
                    &object_path(Path::new(&key))?,
                    &object_path(Path::new(&destination))?,
                )
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn list_json_matching_suffix<T: DeserializeOwned>(
        &self,
        prefix: impl AsRef<Path>,
        suffix: &str,
    ) -> Result<Vec<T>> {
        let mut values = Vec::new();
        for key in self.list_keys(prefix).await? {
            if !key.ends_with(suffix) {
                continue;
            }
            if let Some(value) = self.get_json_if_exists::<T>(Path::new(&key)).await? {
                values.push(value);
            }
        }
        Ok(values)
    }
}

fn write_fsync(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;

    let parent = path
        .parent()
        .with_context(|| format!("durable write target has no parent: {}", path.display()))?;
    std::fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .with_context(|| format!("durable write target has no file name: {}", path.display()))?;
    let temporary = parent.join(format!("{file_name}.tmp.{}", std::process::id()));
    let mut file = std::fs::File::create(&temporary)
        .with_context(|| format!("failed to create {}", temporary.display()))?;
    file.write_all(bytes)?;
    file.sync_all()
        .with_context(|| format!("failed to sync {}", temporary.display()))?;
    drop(file);
    std::fs::rename(&temporary, path)
        .with_context(|| format!("failed to rename into {}", path.display()))?;
    Ok(())
}

fn object_path(path: &Path) -> Result<ObjectPath> {
    ObjectPath::parse(normalize_path(path)).map_err(Into::into)
}

fn object_prefix(prefix: &Path) -> Result<ObjectPath> {
    ObjectPath::parse(normalize_path(prefix)).map_err(Into::into)
}

fn normalize_path(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}
