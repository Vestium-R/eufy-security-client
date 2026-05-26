/** Parsed result from a BLE FF09 frame. */
export interface BleFrame {
    isEncrypted: boolean;
    isResponse: boolean;
    commandCode: number;
    data: Buffer;
}
/** Parsed heartbeat data extracted from a NOTIFY TLV payload. */
export interface HeartbeatData {
    /** Battery percentage (0-100), or -1 if not present in the TLV. */
    battery: number;
    /** Whether the lock is in the locked state. */
    locked: boolean;
    /** Raw lock status byte from the TLV. */
    rawLockStatus: number;
}
/**
 * Handles BLE command frame parsing for the FF09 smart lock protocol.
 *
 * This is the same wire format used by T8506/T8502 smart locks over Bluetooth,
 * but here the frames are transported over MQTT instead of P2P.
 *
 * Frame format: [0xFF, 0x09, ...header(5 bytes), flags(2 bytes), ...data, checksum(1 byte)]
 * Flags: bit 14 = encrypted, bit 11 = response, bits 0-10 = command code
 */
export declare class BleLockProtocol {
    /**
     * Parses a BLE frame from the FF09 protocol.
     *
     * @returns Parsed frame fields, or null if the buffer is not a valid FF09 frame.
     */
    static parseBleFrame(buffer: Buffer): BleFrame | null;
    /**
     * Checks whether a parsed BLE frame is a heartbeat notification.
     *
     * Heartbeats are unencrypted NOTIFY frames containing TLV-encoded
     * battery level and lock status.
     */
    static isHeartbeat(frame: BleFrame): boolean;
    /**
     * Checks whether a parsed BLE frame is a lock/unlock command acknowledgment.
     */
    static isLockCommandResponse(frame: BleFrame): boolean;
    /**
     * Parses a heartbeat TLV payload to extract battery level and lock status.
     *
     * The TLV format uses tag 0xA1 for battery percentage and tag 0xA2 for lock status.
     * A leading byte below 0xA0 is a return code and is skipped.
     *
     * @returns Parsed heartbeat data, or null if no lock status TLV was found.
     */
    static parseHeartbeat(data: Buffer): HeartbeatData | null;
}
