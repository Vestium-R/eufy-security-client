import { TypedEmitter } from "tiny-typed-emitter";
import { SecurityMQTTServiceEvents } from "./interface";
/**
 * Manages BLE-over-MQTT communication with Eufy security locks.
 *
 * Devices that use this service (e.g. Smart Lock C30 / T85D0) send the same
 * BLE command frames as other smart locks (T8506, T8502), but tunneled over
 * a dedicated security MQTT broker instead of P2P or Cloud API.
 *
 * @remarks
 * Auth chain: Security API auth_token -> AIOT mTLS certs -> MQTT connect.
 */
export declare class SecurityMQTTService extends TypedEmitter<SecurityMQTTServiceEvents> {
    private client;
    private connectionState;
    private mqttInfo;
    private clientId;
    private readonly authToken;
    private readonly userId;
    private readonly openudid;
    private readonly country;
    private subscribedLocks;
    /** Maps device serial number to device model for locks awaiting MQTT subscription. */
    private pendingLockSubscriptions;
    private msgSeq;
    /**
     * Creates a new SecurityMQTTService.
     * @param authToken - Security API auth_token (from login_sec).
     * @param userId - Security API user_id (hex string, same as user_center_id).
     * @param openudid - Unique device identifier for the client.
     * @param country - Country code for regional API routing (default: "US").
     */
    constructor(authToken: string, userId: string, openudid: string, country?: string);
    /**
     * Connects to the Eufy security MQTT broker.
     *
     * Fetches mTLS certificates using the Security API auth_token, then establishes an
     * MQTTS connection. Any locks queued via `subscribeLock()` before
     * connection will be subscribed once the connection is established.
     *
     * @param apiBase - The Eufy API base URL, used to determine the regional broker.
     */
    connect(apiBase: string): Promise<void>;
    /** Queues or immediately subscribes to MQTT topics for a lock device. */
    subscribeLock(deviceSN: string, deviceModel: string): void;
    /**
     * Sends a lock or unlock command to a device via the security MQTT broker.
     *
     * Builds a BLE command frame using the shared smart lock command builder, wraps it
     * in the security MQTT envelope, and publishes to the device's request topic.
     *
     * @param deviceSN - Serial number of the target lock device.
     * @param deviceModel - Model identifier used in the MQTT topic (e.g. "T85D0").
     * @param adminUserId - Admin user ID for the lock command.
     * @param shortUserId - Short user ID for the lock command.
     * @param nickName - User nickname included in the lock command.
     * @param channel - BLE channel number.
     * @param sequence - Lock command sequence number.
     * @param lock - true to lock, false to unlock.
     * @returns true if the command was published successfully, false otherwise.
     */
    lockDevice(deviceSN: string, deviceModel: string, adminUserId: string, shortUserId: string, nickName: string, channel: number, sequence: number, lock: boolean): Promise<boolean>;
    isConnected(): boolean;
    close(): void;
    /** Retrieves mTLS certificates from the AIOT endpoint using the Security API auth_token. */
    private authenticate;
    /**
     * Makes an HTTPS request and returns the parsed response.
     *
     * Uses Node's built-in `https` module rather than a third-party library to avoid
     * coupling to HTTPApi internals. Only used for the AIOT cert fetch.
     */
    private httpRequest;
    private getSecurityBrokerHost;
    /**
     * Builds the MQTT client ID from the broker host, user center ID, and openudid.
     *
     * The format matches the official EufySecurity Android app's client ID pattern:
     * `android-eufy_security-{userCenterId}-{openudid}{hostWithoutDots}`
     */
    private buildClientId;
    private getMqttTopic;
    /** Subscribes to request and response MQTT topics for a lock device. */
    private subscribeToLockTopics;
    /** Generates a random 32-character hex string for the MQTT message seed. */
    private static generateSeed;
    /**
     * Builds the MQTT message envelope for sending a command to a lock device.
     *
     * The message format follows the Eufy security MQTT protocol:
     * - `head`: Contains session metadata, sequence number, and command type.
     * - `payload`: JSON-encoded string containing the device SN, account ID, and base64-encoded
     *   transport payload (which wraps the BLE command frame).
     */
    private buildCommandMessage;
    /**
     * Handles incoming MQTT messages from the security broker.
     *
     * Only processes messages on `/res` topics (device responses). Responses containing
     * CMD_TRANSFER_PAYLOAD transport data carry BLE frames from the lock device, which
     * are parsed by {@link BleLockProtocol}.
     */
    private handleMessage;
}
