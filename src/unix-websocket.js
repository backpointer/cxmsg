import crypto from "node:crypto";
import { EventEmitter } from "node:events";
import net from "node:net";

const WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

function encodeFrame(payload, opcode = 0x1) {
  const body = Buffer.isBuffer(payload) ? payload : Buffer.from(payload, "utf8");
  const mask = crypto.randomBytes(4);
  let header;

  if (body.length < 126) {
    header = Buffer.alloc(2);
    header[1] = 0x80 | body.length;
  } else if (body.length <= 0xffff) {
    header = Buffer.alloc(4);
    header[1] = 0x80 | 126;
    header.writeUInt16BE(body.length, 2);
  } else {
    header = Buffer.alloc(10);
    header[1] = 0x80 | 127;
    header.writeBigUInt64BE(BigInt(body.length), 2);
  }
  header[0] = 0x80 | opcode;

  const masked = Buffer.alloc(body.length);
  for (let index = 0; index < body.length; index += 1) {
    masked[index] = body[index] ^ mask[index % 4];
  }
  return Buffer.concat([header, mask, masked]);
}

export class UnixWebSocket extends EventEmitter {
  constructor(socketPath) {
    super();
    this.socketPath = socketPath;
    this.socket = null;
    this.upgraded = false;
    this.closed = false;
    this.buffer = Buffer.alloc(0);
    this.fragments = [];
    this.fragmentOpcode = null;
  }

  connect() {
    if (this.socket) return Promise.resolve();

    return new Promise((resolve, reject) => {
      const key = crypto.randomBytes(16).toString("base64");
      const expectedAccept = crypto
        .createHash("sha1")
        .update(key + WEBSOCKET_GUID)
        .digest("base64");
      let settled = false;

      this.socket = net.createConnection({ path: this.socketPath });
      this.socket.once("connect", () => {
        this.socket.write(
          [
            "GET /rpc HTTP/1.1",
            "Host: localhost",
            "Upgrade: websocket",
            "Connection: Upgrade",
            `Sec-WebSocket-Key: ${key}`,
            "Sec-WebSocket-Version: 13",
            "",
            "",
          ].join("\r\n"),
        );
      });
      this.socket.on("data", (chunk) => {
        if (!this.upgraded) {
          this.buffer = Buffer.concat([this.buffer, chunk]);
          const boundary = this.buffer.indexOf("\r\n\r\n");
          if (boundary === -1) return;

          const header = this.buffer.subarray(0, boundary).toString("utf8");
          this.buffer = this.buffer.subarray(boundary + 4);
          const statusOk = /^HTTP\/1\.1 101\b/m.test(header);
          const acceptMatch = header.match(/^Sec-WebSocket-Accept:\s*(.+)$/im);
          if (!statusOk || acceptMatch?.[1]?.trim() !== expectedAccept) {
            const error = new Error("invalid app-server WebSocket upgrade response");
            settled = true;
            reject(error);
            this.socket.destroy(error);
            return;
          }

          this.upgraded = true;
          settled = true;
          resolve();
        } else {
          this.buffer = Buffer.concat([this.buffer, chunk]);
        }
        this.#decodeFrames();
      });
      this.socket.on("error", (error) => {
        if (!settled) {
          settled = true;
          reject(error);
        } else if (!this.closed) {
          this.emit("error", error);
        }
      });
      this.socket.once("close", () => {
        this.closed = true;
        if (!settled) {
          settled = true;
          reject(new Error("app-server closed during WebSocket upgrade"));
        }
        this.emit("close");
      });
    });
  }

  sendText(text) {
    if (!this.upgraded || !this.socket?.writable) {
      throw new Error("app-server WebSocket is not connected");
    }
    this.socket.write(encodeFrame(text));
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    if (this.upgraded && this.socket?.writable) {
      this.socket.write(encodeFrame(Buffer.alloc(0), 0x8));
    }
    this.socket?.end();
    this.socket?.destroy();
  }

  #decodeFrames() {
    while (this.buffer.length >= 2) {
      const first = this.buffer[0];
      const second = this.buffer[1];
      const final = Boolean(first & 0x80);
      const opcode = first & 0x0f;
      const masked = Boolean(second & 0x80);
      let length = second & 0x7f;
      let offset = 2;

      if (length === 126) {
        if (this.buffer.length < 4) return;
        length = this.buffer.readUInt16BE(2);
        offset = 4;
      } else if (length === 127) {
        if (this.buffer.length < 10) return;
        const wideLength = this.buffer.readBigUInt64BE(2);
        if (wideLength > BigInt(Number.MAX_SAFE_INTEGER)) {
          this.socket.destroy(new Error("WebSocket frame is too large"));
          return;
        }
        length = Number(wideLength);
        offset = 10;
      }

      const maskLength = masked ? 4 : 0;
      if (this.buffer.length < offset + maskLength + length) return;
      const mask = masked ? this.buffer.subarray(offset, offset + 4) : null;
      offset += maskLength;
      const payload = Buffer.from(this.buffer.subarray(offset, offset + length));
      this.buffer = this.buffer.subarray(offset + length);

      if (mask) {
        for (let index = 0; index < payload.length; index += 1) {
          payload[index] ^= mask[index % 4];
        }
      }
      this.#handleFrame(opcode, final, payload);
    }
  }

  #handleFrame(opcode, final, payload) {
    if (opcode === 0x8) {
      this.close();
      return;
    }
    if (opcode === 0x9) {
      if (this.socket?.writable) this.socket.write(encodeFrame(payload, 0xA));
      return;
    }
    if (opcode === 0xA) return;

    if (opcode === 0x1 || opcode === 0x2) {
      if (final) {
        if (opcode === 0x1) this.emit("message", payload.toString("utf8"));
        return;
      }
      this.fragmentOpcode = opcode;
      this.fragments = [payload];
      return;
    }
    if (opcode === 0x0 && this.fragmentOpcode !== null) {
      this.fragments.push(payload);
      if (final) {
        const complete = Buffer.concat(this.fragments);
        if (this.fragmentOpcode === 0x1) {
          this.emit("message", complete.toString("utf8"));
        }
        this.fragmentOpcode = null;
        this.fragments = [];
      }
    }
  }
}
