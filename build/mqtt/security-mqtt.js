"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.SecurityMQTTService = void 0;
const mqtt = __importStar(require("mqtt"));
const https = __importStar(require("https"));
const crypto_1 = require("crypto");
const tiny_typed_emitter_1 = require("tiny-typed-emitter");
const ble_lock_protocol_1 = require("./ble-lock-protocol");
const logging_1 = require("../logging");
const error_1 = require("../error");
const utils_1 = require("../utils");
const utils_2 = require("../p2p/utils");
const types_1 = require("../p2p/types");
const device_1 = require("../http/device");
const AIOT_MQTT_CERT_URL = "https://aiot-clean-api-pr.eufylife.com/app/devicemanage/get_user_mqtt_info";
/** Connection state for the SecurityMQTT service. */
var ConnectionState;
(function (ConnectionState) {
    ConnectionState["DISCONNECTED"] = "disconnected";
    ConnectionState["CONNECTING"] = "connecting";
    ConnectionState["CONNECTED"] = "connected";
})(ConnectionState || (ConnectionState = {}));
/** Command type in the security MQTT message envelope. */
var SecurityMqttCmd;
(function (SecurityMqttCmd) {
    /** Response from device. */
    SecurityMqttCmd[SecurityMqttCmd["DEVICE_RESPONSE"] = 8] = "DEVICE_RESPONSE";
    /** Command sent to device. */
    SecurityMqttCmd[SecurityMqttCmd["SEND_COMMAND"] = 9] = "SEND_COMMAND";
})(SecurityMqttCmd || (SecurityMqttCmd = {}));
/** Command status in the security MQTT message envelope. */
var SecurityMqttCmdStatus;
(function (SecurityMqttCmdStatus) {
    /** Command request. */
    SecurityMqttCmdStatus[SecurityMqttCmdStatus["REQUEST"] = 2] = "REQUEST";
})(SecurityMqttCmdStatus || (SecurityMqttCmdStatus = {}));
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
class SecurityMQTTService extends tiny_typed_emitter_1.TypedEmitter {
    client = null;
    connectionState = ConnectionState.DISCONNECTED;
    mqttInfo = null;
    clientId = "";
    authToken;
    userId;
    openudid;
    country;
    subscribedLocks = new Set();
    /** Maps device serial number to device model for locks awaiting MQTT subscription. */
    pendingLockSubscriptions = new Map();
    msgSeq = 1;
    /**
     * Creates a new SecurityMQTTService.
     * @param authToken - Security API auth_token (from login_sec).
     * @param userId - Security API user_id (hex string, same as user_center_id).
     * @param openudid - Unique device identifier for the client.
     * @param country - Country code for regional API routing (default: "US").
     */
    constructor(authToken, userId, openudid, country = "US") {
        super();
        this.authToken = authToken;
        this.userId = userId;
        this.openudid = openudid;
        this.country = country;
    }
    /**
     * Connects to the Eufy security MQTT broker.
     *
     * Fetches mTLS certificates using the Security API auth_token, then establishes an
     * MQTTS connection. Any locks queued via `subscribeLock()` before
     * connection will be subscribed once the connection is established.
     *
     * @param apiBase - The Eufy API base URL, used to determine the regional broker.
     */
    async connect(apiBase) {
        if (this.connectionState !== ConnectionState.DISCONNECTED) {
            return;
        }
        this.connectionState = ConnectionState.CONNECTING;
        try {
            await this.authenticate();
        }
        catch (err) {
            this.connectionState = ConnectionState.DISCONNECTED;
            const error = (0, error_1.ensureError)(err);
            logging_1.rootMQTTLogger.error("SecurityMQTT authentication failed", { error: (0, utils_1.getError)(error) });
            throw error;
        }
        if (!this.mqttInfo) {
            this.connectionState = ConnectionState.DISCONNECTED;
            throw new Error("SecurityMQTT: No MQTT certificates after authentication");
        }
        const host = this.mqttInfo.endpoint_addr || this.getSecurityBrokerHost(apiBase);
        this.clientId = this.buildClientId(host);
        logging_1.rootMQTTLogger.info(`SecurityMQTT connecting to ${host}:8883`, {
            clientId: this.clientId,
            username: this.mqttInfo.thing_name,
            endpointAddr: this.mqttInfo.endpoint_addr,
            apiBase: apiBase,
        });
        this.client = mqtt.connect({
            host: host,
            port: 8883,
            protocol: "mqtts",
            clientId: this.clientId,
            username: this.mqttInfo.thing_name,
            cert: this.mqttInfo.certificate_pem,
            key: this.mqttInfo.private_key,
            ca: this.mqttInfo.aws_root_ca1_pem,
            rejectUnauthorized: true,
            clean: true,
            keepalive: 60,
            connectTimeout: 30000,
        });
        this.client.on("connect", () => {
            this.connectionState = ConnectionState.CONNECTED;
            logging_1.rootMQTTLogger.info("SecurityMQTT connected successfully");
            this.emit("connect");
            for (const [deviceSN, deviceModel] of this.pendingLockSubscriptions) {
                this.subscribeToLockTopics(deviceSN, deviceModel);
            }
            this.pendingLockSubscriptions.clear();
        });
        this.client.on("close", () => {
            this.connectionState = ConnectionState.DISCONNECTED;
            logging_1.rootMQTTLogger.info("SecurityMQTT connection closed");
            this.emit("close");
        });
        this.client.on("error", (error) => {
            this.connectionState = ConnectionState.DISCONNECTED;
            logging_1.rootMQTTLogger.error("SecurityMQTT error", { error: (0, utils_1.getError)(error) });
        });
        this.client.on("message", (topic, message) => {
            this.handleMessage(topic, message);
        });
    }
    /** Queues or immediately subscribes to MQTT topics for a lock device. */
    subscribeLock(deviceSN, deviceModel) {
        if (this.connectionState === ConnectionState.CONNECTED) {
            this.subscribeToLockTopics(deviceSN, deviceModel);
        }
        else {
            this.pendingLockSubscriptions.set(deviceSN, deviceModel);
        }
    }
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
    lockDevice(deviceSN, deviceModel, adminUserId, shortUserId, nickName, channel, sequence, lock) {
        return new Promise((resolve) => {
            if (!this.client || this.connectionState !== ConnectionState.CONNECTED) {
                logging_1.rootMQTTLogger.error("SecurityMQTT not connected, cannot send lock command");
                resolve(false);
                return;
            }
            const command = (0, utils_2.getSmartLockP2PCommand)(deviceSN, adminUserId, types_1.SmartLockCommand.ON_OFF_LOCK, channel, sequence, device_1.Lock.encodeCmdSmartLockUnlock(adminUserId, lock, nickName, shortUserId), types_1.SmartLockFunctionType.TYPE_2);
            const transPayload = JSON.parse(command.payload.value);
            const message = this.buildCommandMessage(deviceSN, transPayload);
            const topic = this.getMqttTopic(deviceModel, deviceSN, "req");
            logging_1.rootMQTTLogger.debug("SecurityMQTT publishing lock command", {
                topic: topic,
                deviceSN: deviceSN,
                lock: lock,
            });
            this.client.publish(topic, JSON.stringify(message), { qos: 1 }, (err) => {
                if (err) {
                    logging_1.rootMQTTLogger.error("SecurityMQTT publish failed", { error: (0, utils_1.getError)(err) });
                    resolve(false);
                }
                else {
                    logging_1.rootMQTTLogger.info("SecurityMQTT lock command published", {
                        deviceSN: deviceSN,
                        lock: lock,
                    });
                    resolve(true);
                }
            });
        });
    }
    isConnected() {
        return this.connectionState === ConnectionState.CONNECTED;
    }
    close() {
        if (this.client) {
            try {
                this.client.end(true);
            }
            catch (err) {
                const error = (0, error_1.ensureError)(err);
                logging_1.rootMQTTLogger.error("SecurityMQTT close error", { error: (0, utils_1.getError)(error) });
            }
            this.client = null;
            this.connectionState = ConnectionState.DISCONNECTED;
        }
    }
    /** Retrieves mTLS certificates from the AIOT endpoint using the Security API auth_token. */
    async authenticate() {
        logging_1.rootMQTTLogger.debug("SecurityMQTT fetching MQTT certs...");
        const gtoken = (0, crypto_1.createHash)("md5").update(this.userId).digest("hex");
        const mqttCertRes = await this.httpRequest(AIOT_MQTT_CERT_URL, "POST", {
            "Content-Type": "application/json",
            "X-Auth-Token": this.authToken,
            GToken: gtoken,
            "User-Agent": "EufySecurity-Android-4.6.0-1630",
            category: "eufy_security",
            "App-name": "eufy_security",
            openudid: this.openudid,
            language: "en",
            country: this.country,
            "Os-version": "Android",
            "Model-type": "PHONE",
            timezone: "America/New_York",
        }, JSON.stringify({}));
        if (mqttCertRes.data.code !== 0) {
            throw new Error(`MQTT certs failed: ${JSON.stringify(mqttCertRes.data)}`);
        }
        this.mqttInfo = mqttCertRes.data.data;
        logging_1.rootMQTTLogger.info("SecurityMQTT certs OK", {
            thingName: this.mqttInfo.thing_name,
            endpointAddr: this.mqttInfo.endpoint_addr,
        });
    }
    /**
     * Makes an HTTPS request and returns the parsed response.
     *
     * Uses Node's built-in `https` module rather than a third-party library to avoid
     * coupling to HTTPApi internals. Only used for the AIOT cert fetch.
     */
    httpRequest(url, method, headers, body) {
        return new Promise((resolve, reject) => {
            const urlObj = new URL(url);
            const reqOpts = {
                hostname: urlObj.hostname,
                port: urlObj.port || 443,
                path: urlObj.pathname + urlObj.search,
                method: method,
                headers: headers,
            };
            const req = https.request(reqOpts, (res) => {
                let data = "";
                res.on("data", (chunk) => (data += chunk));
                res.on("end", () => {
                    try {
                        resolve({ status: res.statusCode || 0, data: JSON.parse(data) });
                    }
                    catch {
                        resolve({ status: res.statusCode || 0, data: data });
                    }
                });
            });
            req.on("error", reject);
            if (body) {
                req.write(body);
            }
            req.end();
        });
    }
    getSecurityBrokerHost(apiBase) {
        if (apiBase.includes("-eu.")) {
            return "security-mqtt-eu.anker.com";
        }
        return "security-mqtt-us.anker.com";
    }
    /**
     * Builds the MQTT client ID from the broker host, user center ID, and openudid.
     *
     * The format matches the official EufySecurity Android app's client ID pattern:
     * `android-eufy_security-{userCenterId}-{openudid}{hostWithoutDots}`
     */
    buildClientId(host) {
        const hostNoPunctuation = host.replace(/[.\-]/g, "");
        return `android-eufy_security-${this.userId}-${this.openudid}${hostNoPunctuation}`;
    }
    getMqttTopic(deviceModel, deviceSN, direction) {
        return `cmd/eufy_security/${deviceModel}/${deviceSN}/${direction}`;
    }
    /** Subscribes to request and response MQTT topics for a lock device. */
    subscribeToLockTopics(deviceSN, deviceModel) {
        if (!this.client || this.subscribedLocks.has(deviceSN)) {
            return;
        }
        const topics = [this.getMqttTopic(deviceModel, deviceSN, "res"), this.getMqttTopic(deviceModel, deviceSN, "req")];
        for (const topic of topics) {
            this.client.subscribe(topic, { qos: 1 }, (err) => {
                if (err) {
                    logging_1.rootMQTTLogger.error(`SecurityMQTT subscribe failed: ${topic}`, { error: (0, utils_1.getError)(err) });
                }
                else {
                    logging_1.rootMQTTLogger.info(`SecurityMQTT subscribed: ${topic}`);
                }
            });
        }
        this.subscribedLocks.add(deviceSN);
    }
    /** Generates a random 32-character hex string for the MQTT message seed. */
    static generateSeed() {
        return Array.from({ length: 16 }, () => Math.floor(Math.random() * 256)
            .toString(16)
            .padStart(2, "0")).join("");
    }
    /**
     * Builds the MQTT message envelope for sending a command to a lock device.
     *
     * The message format follows the Eufy security MQTT protocol:
     * - `head`: Contains session metadata, sequence number, and command type.
     * - `payload`: JSON-encoded string containing the device SN, account ID, and base64-encoded
     *   transport payload (which wraps the BLE command frame).
     */
    buildCommandMessage(deviceSN, transPayload) {
        const sessionId = Math.random().toString(16).substring(2, 6);
        const timestamp = Math.trunc(Date.now() / 1000);
        const transportPayload = {
            cmd: transPayload.cmd,
            mChannel: transPayload.mChannel,
            mValue3: transPayload.mValue3,
            payload: transPayload.payload,
        };
        const transportBase64 = Buffer.from(JSON.stringify(transportPayload)).toString("base64");
        const devicePayload = {
            account_id: transPayload.account_id,
            device_sn: deviceSN,
            trans: transportBase64,
        };
        return {
            head: {
                version: "1.0.0.1",
                client_id: this.clientId,
                sess_id: sessionId,
                msg_seq: this.msgSeq++,
                seed: SecurityMQTTService.generateSeed(),
                timestamp: timestamp,
                cmd_status: SecurityMqttCmdStatus.REQUEST,
                cmd: SecurityMqttCmd.SEND_COMMAND,
                sign_code: 0,
            },
            payload: JSON.stringify(devicePayload),
        };
    }
    /**
     * Handles incoming MQTT messages from the security broker.
     *
     * Only processes messages on `/res` topics (device responses). Responses containing
     * CMD_TRANSFER_PAYLOAD transport data carry BLE frames from the lock device, which
     * are parsed by {@link BleLockProtocol}.
     */
    handleMessage(topic, message) {
        try {
            const parsed = JSON.parse(message.toString());
            if (!topic.endsWith("/res")) {
                return;
            }
            if (typeof parsed.payload !== "string") {
                return;
            }
            const payload = JSON.parse(parsed.payload);
            if (!payload.trans) {
                return;
            }
            const transportData = JSON.parse(Buffer.from(payload.trans, "base64").toString("utf8"));
            if (transportData.cmd !== types_1.CommandType.CMD_TRANSFER_PAYLOAD) {
                return;
            }
            const lockPayload = transportData.payload;
            if (!lockPayload || !lockPayload.lock_payload) {
                return;
            }
            const deviceSN = lockPayload.dev_sn || payload.device_sn;
            const bleBuffer = Buffer.from(lockPayload.lock_payload, "hex");
            const frame = ble_lock_protocol_1.BleLockProtocol.parseBleFrame(bleBuffer);
            if (!frame) {
                return;
            }
            logging_1.rootMQTTLogger.debug("SecurityMQTT received BLE frame", {
                deviceSN: deviceSN,
                encrypted: frame.isEncrypted,
                response: frame.isResponse,
                commandCode: frame.commandCode,
            });
            if (ble_lock_protocol_1.BleLockProtocol.isHeartbeat(frame)) {
                const heartbeat = ble_lock_protocol_1.BleLockProtocol.parseHeartbeat(frame.data);
                if (heartbeat) {
                    logging_1.rootMQTTLogger.info("SecurityMQTT heartbeat", {
                        deviceSN: deviceSN,
                        locked: heartbeat.locked,
                        battery: heartbeat.battery,
                        rawStatus: heartbeat.rawLockStatus,
                    });
                    this.emit("lock status", deviceSN, heartbeat.locked, heartbeat.battery);
                }
            }
            else if (ble_lock_protocol_1.BleLockProtocol.isLockCommandResponse(frame)) {
                logging_1.rootMQTTLogger.info("SecurityMQTT received lock command response", {
                    deviceSN: deviceSN,
                    commandCode: frame.commandCode,
                });
                this.emit("command response", deviceSN, true);
            }
        }
        catch (err) {
            const error = (0, error_1.ensureError)(err);
            logging_1.rootMQTTLogger.error("SecurityMQTT message parse error", { error: (0, utils_1.getError)(error) });
        }
    }
}
exports.SecurityMQTTService = SecurityMQTTService;
//# sourceMappingURL=security-mqtt.js.map