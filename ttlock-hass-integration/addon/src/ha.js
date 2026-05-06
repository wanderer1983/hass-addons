'use strict';

const mqtt = require('async-mqtt');
const manager = require('./manager');
const { LockedStatus } = require('ttlock-sdk-js');

class HomeAssistant {
    /**
     * @param {Object} options
     * @param {string} options.mqttUrl
     * @param {string} options.mqttUser
     * @param {string} options.mqttPass
     * @param {string} options.discovery_prefix
     */
    constructor(options) {
        this.mqttUrl = options.mqttUrl;
        this.mqttUser = options.mqttUser;
        this.mqttPass = options.mqttPass;
        this.discovery_prefix = options.discovery_prefix || "homeassistant";
        this.configuredLocks = new Set();
        this.connected = false;

        // Lock state events — require connection, update full state
        manager.on("lockPaired", this._onLockPaired.bind(this));
        manager.on("lockConnected", this._onLockConnected.bind(this));
        manager.on("lockUnlock", this._onLockUnlock.bind(this));
        manager.on("lockLock", this._onLockLock.bind(this));

        // lockDiscovered fires as soon as the lock is seen advertising —
        // no BLE connection needed. Use it to configure the HA entity early
        // so battery/RSSI sensors appear immediately on startup.
        manager.on("lockDiscovered", this._onLockDiscovered.bind(this));

        // Battery update fired on every advertisement — no connection needed.
        // Also fired when battery changes during a connected session.
        manager.on("lockBatteryUpdated", this._onLockBatteryUpdated.bind(this));

        // lockUpdated covers settings changes (autolock, audio, etc.)
        manager.on("lockUpdated", this._onLockUpdated.bind(this));
    }

    async connect() {
        if (!this.connected) {
            this.client = await mqtt.connectAsync(this.mqttUrl, {
                username: this.mqttUser,
                password: this.mqttPass
            });
            this.client.on("message", this._onMQTTMessage.bind(this));
            await this.client.subscribe("ttlock/+/set");
            this.connected = true;
            console.log("MQTT connected");
        }
    }

    /**
     * Construct a unique ID for a lock based on MAC address
     * @param {import('ttlock-sdk-js').TTLock} lock
     */
    getLockId(lock) {
        return lock.getAddress().split(":").join("").toLowerCase();
    }

    /**
     * Publish MQTT discovery config for a lock.
     * Creates three entities in HA:
     *   - lock entity (lock/unlock control)
     *   - battery sensor (updated from advertisements, no connection needed)
     *   - RSSI sensor
     *
     * Safe to call multiple times — only publishes once per lock address.
     *
     * @param {import('ttlock-sdk-js').TTLock} lock
     */
    async configureLock(lock) {
        if (!this.connected) return;
        if (this.configuredLocks.has(lock.getAddress())) return;

        const id = this.getLockId(lock);
        const name = lock.getName();
        const device = {
            identifiers: ["ttlock_" + id],
            name: name,
            manufacturer: lock.getManufacturer(),
            model: lock.getModel(),
            sw_version: lock.getFirmware()
        };

        // Lock control entity
        const configLockTopic = `${this.discovery_prefix}/lock/${id}/lock/config`;
        const lockPayload = {
            unique_id: "ttlock_" + id,
            name: name,
            device: device,
            state_topic: "ttlock/" + id,
            command_topic: "ttlock/" + id + "/set",
            payload_lock: "LOCK",
            payload_unlock: "UNLOCK",
            state_locked: "LOCK",
            state_unlocked: "UNLOCK",
            value_template: "{{ value_json.state }}",
            optimistic: false,
            retain: false
        };
        await this._publish(configLockTopic, lockPayload, { retain: true });

        // Battery sensor — updated from BLE advertisements without connecting
        const configBatteryTopic = `${this.discovery_prefix}/sensor/${id}/battery/config`;
        const batteryPayload = {
            unique_id: "ttlock_" + id + "_battery",
            name: name + " Battery",
            device: device,
            device_class: "battery",
            state_class: "measurement",
            unit_of_measurement: "%",
            state_topic: "ttlock/" + id,
            value_template: "{{ value_json.battery }}",
            // Entity category means it shows in device info rather than main dashboard
            entity_category: "diagnostic"
        };
        await this._publish(configBatteryTopic, batteryPayload, { retain: true });

        // RSSI sensor
        const configRssiTopic = `${this.discovery_prefix}/sensor/${id}/rssi/config`;
        const rssiPayload = {
            unique_id: "ttlock_" + id + "_rssi",
            name: name + " RSSI",
            device: device,
            device_class: "signal_strength",
            state_class: "measurement",
            unit_of_measurement: "dBm",
            icon: "mdi:signal",
            state_topic: "ttlock/" + id,
            value_template: "{{ value_json.rssi }}",
            entity_category: "diagnostic"
        };
        await this._publish(configRssiTopic, rssiPayload, { retain: true });

        this.configuredLocks.add(lock.getAddress());
        console.log(`MQTT configured lock: ${name} (${lock.getAddress()})`);
    }

    /**
     * Publish current state for a lock.
     * Called after a connection — includes lock state, battery, RSSI.
     * @param {import('ttlock-sdk-js').TTLock} lock
     */
    async updateLockState(lock) {
        if (!this.connected) return;
        const id = this.getLockId(lock);
        const stateTopic = "ttlock/" + id;
        const lockedStatus = await lock.getLockStatus();

        const payload = {
            battery: lock.getBattery ? lock.getBattery() : lock.batteryCapacity,
            rssi: lock.getRssi ? lock.getRssi() : lock.rssi
        };
        if (lockedStatus !== LockedStatus.UNKNOWN) {
            payload.state = lockedStatus === LockedStatus.LOCKED ? "LOCK" : "UNLOCK";
        }

        await this._publish(stateTopic, payload, { retain: true });
    }

    /**
     * Publish a battery+RSSI update without requiring a lock state read.
     * Safe to call from advertisement events (no BLE connection needed).
     * @param {import('ttlock-sdk-js').TTLock} lock
     */
    async updateLockBattery(lock) {
        if (!this.connected) return;
        const id = this.getLockId(lock);
        const stateTopic = "ttlock/" + id;

        const battery = lock.getBattery ? lock.getBattery() : lock.batteryCapacity;
        const rssi = lock.getRssi ? lock.getRssi() : lock.rssi;

        // Only publish if we have valid data
        if (battery === null || battery === undefined || battery < 0) return;

        // Merge with retained state so we don't overwrite the lock state value
        // by publishing a partial payload. We publish only battery+rssi and let
        // HA's value_template extract what it needs from the retained message.
        // To avoid overwriting state, we publish to a sub-topic instead.
        const batteryTopic = "ttlock/" + id + "/battery";
        await this._publish(batteryTopic, { battery, rssi }, { retain: true });

        // Also publish to main state topic to keep battery in sync there.
        // We don't know the current lock state here, so we omit the state field —
        // HA will keep the last known state value since we use retain: true.
        await this._publish(stateTopic, { battery, rssi }, { retain: true });
    }

    // ---------------------------------------------------------------------------
    // Event handlers
    // ---------------------------------------------------------------------------

    /**
     * Lock seen advertising for the first time — configure HA entity immediately.
     * No BLE connection needed. Battery/RSSI will start updating right away.
     */
    async _onLockDiscovered(lock) {
        await this.configureLock(lock);
        // Publish initial battery reading from the advertisement
        await this.updateLockBattery(lock);
    }

    async _onLockPaired(lock) {
        await this.configureLock(lock);
    }

    async _onLockConnected(lock) {
        await this.configureLock(lock);
        await this.updateLockState(lock);
    }

    async _onLockUnlock(lock) {
        await this.updateLockState(lock);
    }

    async _onLockLock(lock) {
        await this.updateLockState(lock);
    }

    /**
     * Battery updated from BLE advertisement — no connection needed.
     * Fires frequently while lock is in range.
     */
    async _onLockBatteryUpdated(lock) {
        // Ensure the lock is configured in HA first
        await this.configureLock(lock);
        await this.updateLockBattery(lock);
    }

    /**
     * Lock settings changed (autolock, audio, etc.)
     */
    async _onLockUpdated(lock) {
        await this.updateLockState(lock);
    }

    // ---------------------------------------------------------------------------
    // MQTT command handler
    // ---------------------------------------------------------------------------

    _onMQTTMessage(topic, message) {
        const topicArr = topic.split("/");
        if (topicArr.length === 3 && topicArr[0] === "ttlock" && topicArr[2] === "set" && topicArr[1].length === 12) {
            // Convert 12-char hex ID back to MAC address (e.g. "06a3633671f4" -> "06:A3:63:36:71:F4")
            let address = "";
            for (let i = 0; i < topicArr[1].length; i++) {
                address += topicArr[1][i];
                if (i < topicArr[1].length - 1 && i % 2 === 1) address += ":";
            }
            address = address.toUpperCase();

            const command = message.toString("utf8");
            if (process.env.MQTT_DEBUG === "1") {
                console.log("MQTT command:", address, command);
            }
            switch (command) {
                case "LOCK":
                    manager.lockLock(address);
                    break;
                case "UNLOCK":
                    manager.unlockLock(address);
                    break;
            }
        } else if (process.env.MQTT_DEBUG === "1") {
            console.log("MQTT unknown topic:", topic, message.toString("utf8"));
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    async _publish(topic, payload, options = {}) {
        try {
            const message = JSON.stringify(payload);
            if (process.env.MQTT_DEBUG === "1") {
                console.log("MQTT Publish", topic, message);
            }
            await this.client.publish(topic, message, options);
        } catch (error) {
            console.error("MQTT publish error:", error);
        }
    }
}

module.exports = HomeAssistant;