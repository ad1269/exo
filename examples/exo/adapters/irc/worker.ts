import { createHash } from "node:crypto";
import net from "node:net";
import readline from "node:readline";
import tls from "node:tls";
import process from "node:process";
import inputReadline from "node:readline/promises";

import {
  adapterConfig,
  booleanField,
  numberField,
  optionalStringField,
  parseWorkerCommand,
  stringField,
  writeWorkerEvent,
} from "../protocol";
import { sendOnce, sentMarkerDir } from "../sent-marker";
import {
  isIrcErrorNumeric,
  parseIrcLine,
  shouldTrigger as shouldTriggerForPolicy,
  type IrcTriggerPolicy,
} from "./irc";

const config = adapterConfig();
const server = stringField(config, "server");
const port = numberField(config, "port");
const useTls = booleanField(config, "tls");
const nick = stringField(config, "nick");
const username = stringField(config, "username");
const realname = stringField(config, "realname");
const channel = stringField(config, "channel");
const password =
  process.env.EXO_IRC_PASSWORD ?? optionalStringField(config, "password");
const trigger = stringField(config, "trigger") as IrcTriggerPolicy;
const sentDir = sentMarkerDir();

if (port <= 0 || !Number.isInteger(port)) {
  throw new Error("IRC port must be a positive integer");
}
if (!channel.startsWith("#")) {
  throw new Error("IRC channel must start with '#'");
}
if (trigger !== "mention" && trigger !== "all_messages") {
  throw new Error("IRC trigger must be mention or all_messages");
}

const socket = useTls
  ? tls.connect({ host: server, port, servername: server })
  : net.connect({ host: server, port });

socket.setEncoding("utf8");
socket.on("error", (error) => {
  writeWorkerEvent({ type: "error", message: error.message });
});
socket.on("close", () => {
  writeWorkerEvent({ type: "disconnected", reason: "socket closed" });
  process.exit(1);
});

await onceConnected(socket);
if (password) {
  writeIrcCommandDetached(`PASS ${password}`);
}
writeIrcCommandDetached(`NICK ${nick}`);
writeIrcCommandDetached(`USER ${username} 0 * :${realname}`);

const lines = readline.createInterface({
  input: socket,
  crlfDelay: Number.POSITIVE_INFINITY,
});

lines.on("error", (error) => {
  writeWorkerEvent({
    type: "error",
    message: `IRC socket read error: ${error.message}`,
  });
});

let registered = false;
let joined = false;

lines.on("line", (raw) => {
  const line = parseIrcLine(raw);
  if (line.type === "ping") {
    writeIrcCommandDetached(`PONG :${line.token}`);
    return;
  }
  if (isIrcErrorNumeric(raw)) {
    writeWorkerEvent({ type: "error", message: raw });
    return;
  }
  if (!registered && raw.includes(` 001 ${nick} `)) {
    registered = true;
    writeIrcCommandDetached(`JOIN ${channel}`);
    return;
  }
  if (!joined && isJoinConfirmation(raw)) {
    joined = true;
    process.stderr.write(
      `[irc-adapter] connected ${server}/${channel} as ${nick}\n`,
    );
    writeWorkerEvent({
      type: "connected",
      subject: `${server}/${channel}`,
      metadata: { server, channel, nick },
    });
    return;
  }
  if (line.type === "privmsg" && line.message.target === channel) {
    if (
      !shouldTriggerForPolicy(
        trigger,
        nick,
        line.message.nick,
        line.message.text,
      )
    ) {
      return;
    }
    process.stderr.write(
      `[irc-adapter] received message from ${line.message.nick} in ${channel}\n`,
    );
    writeWorkerEvent({
      type: "message",
      target: channel,
      sender: line.message.nick,
      text: line.message.text,
      message_id: ircMessageId(server, channel, line.message.raw),
      metadata: { raw: line.message.raw, server, channel },
    });
  }
});

function ircMessageId(server: string, channel: string, raw: string): string {
  return createHash("sha256")
    .update(server)
    .update("\0")
    .update(channel)
    .update("\0")
    .update(raw)
    .digest("hex");
}

const input = inputReadline.createInterface({
  input: process.stdin,
  crlfDelay: Number.POSITIVE_INFINITY,
});

input.on("error", (error) => {
  writeWorkerEvent({
    type: "error",
    message: `IRC adapter command stream error: ${error.message}`,
  });
});

for await (const line of input) {
  if (line.trim().length === 0) {
    continue;
  }
  let commandId: string | null = null;
  try {
    const command = parseWorkerCommand(JSON.parse(line));
    commandId = command.id;
    await sendOnce(sentDir, command.id, async () => {
      process.stderr.write(`[irc-adapter] sending message to ${channel}\n`);
      await writeIrcCommand(`PRIVMSG ${channel} :${command.text}`);
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    writeWorkerEvent({
      type: "error",
      message,
    });
    if (commandId !== null) {
      writeWorkerEvent({
        type: "command_nack",
        command_id: commandId,
        message,
      });
    }
  }
}

function onceConnected(socket: net.Socket): Promise<void> {
  if (socket.readyState === "open") {
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("secureConnect", resolve);
    socket.once("error", reject);
  });
}

// Resolves once the socket has accepted the bytes; the detached variant below
// is for the writes that have no command to nack.
function writeIrcCommand(command: string): Promise<void> {
  return new Promise((resolve, reject) => {
    socket.write(`${command}\r\n`, (error) => {
      if (error) {
        reject(new Error(`IRC socket write error: ${error.message}`));
        return;
      }
      resolve();
    });
  });
}

function writeIrcCommandDetached(command: string): void {
  writeIrcCommand(command).catch((error: Error) => {
    writeWorkerEvent({ type: "error", message: error.message });
  });
}

function isJoinConfirmation(raw: string): boolean {
  return (
    raw.includes(` JOIN ${channel}`) ||
    raw.includes(` JOIN :${channel}`) ||
    raw.includes(` 366 ${nick} ${channel} `)
  );
}
