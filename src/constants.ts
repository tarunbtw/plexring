/**
 * Memory layout, cache-line separation, and binary slot constants for plexring.
 *
 * Header Layout (384 bytes, 3 x 128-byte cache lines):
 * Line 0 (Bytes 0..127):   WRITE_IDX (Int32[0])  - Modified exclusively by Producer
 * Line 1 (Bytes 128..255): READ_IDX  (Int32[32]) - Modified exclusively by Consumer
 * Line 2 (Bytes 256..383): SLEEP_FLAG(Int32[64]) - Modified by Consumer before futex park
 *
 * Slot Layout (256 bytes per slot, 1024 slots):
 * Offset +0:      jobType (u8) on submit ring / status (u8) on result ring (0 = OK, 1 = Error)
 * Offset +1..+4:  requestId (u32, little-endian)
 * Offset +5..+6:  payloadLen (u16, little-endian)
 * Offset +7..+255: payload bytes (max 249 bytes)
 */

// 128-byte alignment eliminates false sharing on both Intel/AMD (64B) and Apple Silicon (128B)
export const CACHE_LINE_BYTES = 128;
export const HEADER_BYTES = 384; // 3 cache lines

// Int32Array indices (byteOffset / 4)
export const WRITE_IDX = 0;   // Byte offset 0..127
export const READ_IDX = 32;   // Byte offset 128..255
export const SLEEP_FLAG = 64; // Byte offset 256..383

// Ring Capacity & Bitwise Masking
export const CAPACITY = 1024; // Must be power of 2
export const MASK = CAPACITY - 1; // 1023
export const SLOT_SIZE = 256; // 1 << 8
export const SLOT_SHIFT = 8;
export const DATA_BYTES = CAPACITY * SLOT_SIZE; // 262,144 bytes
export const TOTAL_RING_BYTES = HEADER_BYTES + DATA_BYTES; // 262,528 bytes

// Slot Schema
export const SLOT_HEADER_BYTES = 7;
export const MAX_PAYLOAD = SLOT_SIZE - SLOT_HEADER_BYTES; // 249 bytes

export const OFFSET_JOB_TYPE = 0;
export const OFFSET_REQUEST_ID = 1;
export const OFFSET_PAYLOAD_LEN = 5;
export const OFFSET_PAYLOAD = 7;

// Status & Message Protocols
export const STATUS_OK = 0;
export const STATUS_ERROR = 1;

export const MSG_SHUTDOWN = 'shutdown';
export const MSG_FALLBACK_JOB = 'fallback_job';
export const MSG_FALLBACK_RES = 'fallback_res';

// Worker Loop Synchronization Thresholds
export const SPIN_LIMIT = 2000;      // User-space spin iterations before kernel futex park
export const FUTEX_TIMEOUT_MS = 50;  // Periodic timeout to service libuv event loop and fallback I/O


