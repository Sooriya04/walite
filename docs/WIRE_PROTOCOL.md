# walite Wire Protocol Specification

## 1. Framing
All network frames transmitted over TCP in `walited` adhere to length-prefixed binary framing:

```
+--------------------------------------------------------+
| FrameLen (4B) | Magic (2B) | Version (1B) | OpCode (1B)|
+--------------------------------------------------------+
| Payload (FrameLen - 4 bytes)                           |
+--------------------------------------------------------+
```

- **FrameLen** (`uint32`, Big-Endian): Total length of the frame including headers.
- **Magic** (`uint16`): Fixed magic identifier `0x574C` (ASCII for 'W', 'L').
- **Version** (`uint8`): Protocol version (`0x01`).
- **OpCode** (`uint8`): Request/Response operation identifier.

## 2. Operation Codes (OpCodes)
- `0x01`: `PRODUCE_REQ` / `0x81`: `PRODUCE_RESP`
- `0x02`: `CONSUME_REQ` / `0x82`: `CONSUME_RESP`
- `0x03`: `OFFSET_REQ`  / `0x83`: `OFFSET_RESP`
- `0x04`: `COMMIT_REQ`  / `0x84`: `COMMIT_RESP`
- `0x05`: `PING_REQ`    / `0x85`: `PONG_RESP`
