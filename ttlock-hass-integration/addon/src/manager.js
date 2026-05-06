'use strict';

const EventEmitter = require('events');
const store = require("./store");
const { TTLockClient, AudioManage, LockedStatus, LogOperateCategory, LogOperateNames } = require("ttlock-sdk-js");

const RSSI_STALENESS_MS = 120_000; // drop RSSI reading after 2 minutes
const PROXY_TIMEOUT_MS = 60_000; // proxy considered dead after 60s no activity
const COMMAND_TIMEOUT_MS = 120_000; // give up waiting for lock to wake after 2 minutes

/**
 * Sleep for ms milliseconds
 * @param {number} ms
 */
async function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Architecture:
 *
 * TTLock devices are battery-powered and sleep between uses. They cannot be
 * woken remotely — they only accept BLE connections when they are awake
 * (after physical interaction like touching the keypad).
 *
 * Strategy:
 * - Monitor mode runs continuously on all proxies, listening for advertisements
 * - When a command is needed (lock/unlock/credentials), it is placed in a
 *   per-lock command queue with a Promise that resolves when executed
 * - When the lock wakes up and advertises, _onFoundLock fires, sees the pending
 *   command, connects and executes it
 * - If the lock doesn't wake within COMMAND_TIMEOUT_MS, the promise resolves false
 *
 * Battery level is broadcast in every advertisement — updated passively without
 * ever needing a BLE connection.
 *
 * Lock data is stored as a flat array (same format as original).
 * RSSI table is ephemeral — never saved.
 *
 * Events emitted:
 * - lockListChanged      - lock list changed
 * - lockDiscovered       - paired lock seen for first time (use for HA config)
 * - lockPaired           - new lock successfully paired
 * - lockConnected        - connection established
 * - lockLock             - lock was locked
 * - lockUnlock           - lock was unlocked
 * - lockUpdated          - lock settings changed
 * - lockBatteryUpdated   - battery level updated (from advertisement, no connection needed)
 * - lockWaiting          - command queued, waiting for lock to wake
 * - scanStart            - BLE scan started
 * - scanStop             - BLE scan stopped
 */
class Manager extends EventEmitter {
    constructor() {
        super();
        this.startupStatus = -1;

        /**
         * One entry per proxy host.
         * { id: string, client: TTLockClient, lastSeen: number }
         * @type {Array<{ id: string, client: TTLockClient, lastSeen: number }>}
         */
        this.proxies = [];

        this.scanning = false;
        /** @type {NodeJS.Timeout} */
        this.scanTimer = undefined;

        /**
         * Canonical paired lock objects — ONE per address, flat, same as original.
         * @type {Map<string, import('ttlock-sdk-js').TTLock>}
         */
        this.pairedLocks = new Map();

        /**
         * New (unpaired) locks waiting for user to press Pair.
         * @type {Map<string, import('ttlock-sdk-js').TTLock>}
         */
        this.newLocks = new Map();

        /**
         * Locks to connect to after scan stops (first-time data read).
         * @type {Set<string>}
         */
        this.connectQueue = new Set();

        /**
         * EPHEMERAL — never saved.
         * RSSI table: lockAddress -> Array<{ proxyId, rssi, lastSeen }>
         * @type {Map<string, Array<{ proxyId: string, rssi: number, lastSeen: number }>>}
         */
        this.lockRssiMap = new Map();

        /**
         * Per-lock command queue.
         * Only ONE pending command per lock at a time (new command replaces old).
         * @type {Map<string, { action: Function, resolve: Function, reject: Function, timer: NodeJS.Timeout, description: string }>}
         */
        this.commandQueue = new Map();

        /**
         * Prevents concurrent connect attempts to the same lock.
         * @type {Set<string>}
         */
        this._connecting = new Set();

        /** @type {'none'|'noble'} */
        this.gateway = 'none';
        this.gateway_host = "";
        this.gateway_port = 0;
        this.gateway_key = "";
        this.gateway_user = "";
        this.gateway_pass = "";

        // Periodically prune stale RSSI entries
        this._pruneInterval = setInterval(() => this._pruneStaleRssi(), 30_000);
    }

    async init() {
        if (this.proxies.length === 0) {
            try {
                const hosts = this.gateway === "noble"
                    ? this.gateway_host.split(',').map(h => h.trim()).filter(Boolean)
                    : ["local"];

                // Per-proxy AES keys: comma-separated, matched by index, last key reused if fewer
                const keys = this.gateway_key.split(',').map(k => k.trim()).filter(Boolean);
                const keyFor = (i) => keys[i] || keys[keys.length - 1] || "";

                const lockData = store.getLockData();

                for (let i = 0; i < hosts.length; i++) {
                    const host = hosts[i];
                    let clientOptions = {};
                    if (this.gateway === "noble") {
                        clientOptions.scannerType = "noble-websocket";
                        clientOptions.scannerOptions = {
                            websocketHost: host,
                            websocketPort: this.gateway_port,
                            websocketAesKey: keyFor(i),
                            websocketUsername: this.gateway_user,
                            websocketPassword: this.gateway_pass
                        };
                    }

                    const proxyId = host;
                    const client = new TTLockClient(clientOptions);
                    const proxy = { id: proxyId, client, lastSeen: Date.now() };
                    this.proxies.push(proxy);

                    client.setLockData(lockData);

                    client.on("ready", () => {
                        proxy.lastSeen = Date.now();
                        client.startMonitor();
                    });

                    client.on("foundLock", (lock) => {
                        proxy.lastSeen = Date.now();
                        this._updateRssi(proxyId, lock.getAddress(), lock.rssi);
                        this._onFoundLock(lock, proxyId);
                    });

                    client.on("scanStart", () => { proxy.lastSeen = Date.now(); this._onScanStarted(); });
                    client.on("scanStop", () => { proxy.lastSeen = Date.now(); this._onScanStopped(); });
                    client.on("monitorStart", () => { proxy.lastSeen = Date.now(); console.log(`[${proxyId}] Monitor started`); });
                    client.on("monitorStop", () => console.log(`[${proxyId}] Monitor stopped`));
                    client.on("updatedLockData", this._onUpdatedLockData.bind(this));

                    const adapterReady = await client.prepareBTService();
                    if (adapterReady) {
                        this.startupStatus = 0;
                        console.log(`[${proxyId}] BT adapter ready`);
                    } else {
                        this.startupStatus = 1;
                        console.error(`[${proxyId}] BT adapter NOT ready`);
                    }
                }
            } catch (error) {
                console.error(error);
                this.startupStatus = 1;
            }
        }
    }

    updateClientLockDataFromStore() {
        const lockData = store.getLockData();
        this.proxies.forEach(p => p.client.setLockData(lockData));
    }

    setNobleGateway(gateway_host, gateway_port, gateway_key, gateway_user, gateway_pass) {
        this.gateway = "noble";
        this.gateway_host = gateway_host;
        this.gateway_port = gateway_port;
        this.gateway_key = gateway_key;
        this.gateway_user = gateway_user;
        this.gateway_pass = gateway_pass;
    }

    getStartupStatus() { return this.startupStatus; }
    getIsScanning() { return this.scanning; }
    getPairedVisible() { return this.pairedLocks; }
    getNewVisible() { return this.newLocks; }

    getProxyStatus() {
        const now = Date.now();
        return {
            proxies: this.proxies.map(p => ({
                id: p.id,
                alive: now - p.lastSeen < PROXY_TIMEOUT_MS,
                lastSeenMs: now - p.lastSeen
            })),
            locks: Object.fromEntries(
                [...this.lockRssiMap].map(([addr, entries]) => [addr, entries.map(e => ({
                    proxyId: e.proxyId,
                    rssi: e.rssi,
                    staleMs: now - e.lastSeen,
                    fresh: now - e.lastSeen < RSSI_STALENESS_MS
                }))])
            ),
            pendingCommands: [...this.commandQueue.keys()]
        };
    }

    // ---------------------------------------------------------------------------
    // Scan control
    // ---------------------------------------------------------------------------

    async startScan() {
        if (this.scanning) return false;
        for (const p of this.proxies) await p.client.stopMonitor();
        let anyStarted = false;
        for (const p of this.proxies) {
            if (await p.client.startScanLock()) anyStarted = true;
        }
        if (anyStarted) this._scanTimer();
        return anyStarted;
    }

    async stopScan() {
        if (!this.scanning) return false;
        if (this.scanTimer) {
            clearTimeout(this.scanTimer);
            this.scanTimer = undefined;
        }
        let anyStopped = false;
        for (const p of this.proxies) {
            if (await p.client.stopScanLock()) anyStopped = true;
        }
        return anyStopped;
    }

    // ---------------------------------------------------------------------------
    // Command queue
    // ---------------------------------------------------------------------------

    /**
     * Enqueue an action for a lock. Returns a Promise that resolves when the
     * action executes (after the lock wakes) or resolves false on timeout.
     *
     * @param {string} address
     * @param {string} description
     * @param {(lock: import('ttlock-sdk-js').TTLock) => Promise<any>} action
     * @returns {Promise<any>}
     */
    _enqueueCommand(address, description, action) {
        const lock = this.pairedLocks.get(address);
        if (!lock) return Promise.resolve(false);

        // If already connected, run immediately
        if (lock.isConnected()) {
            console.log(`[Queue] Lock ${address} already connected, running "${description}" immediately`);
            return action(lock).catch(err => { console.error(err); return false; });
        }

        // Cancel any existing pending command for this lock
        this._cancelCommand(address, "replaced by newer command");

        return new Promise((resolve) => {
            const timer = setTimeout(() => {
                this.commandQueue.delete(address);
                console.warn(`[Queue] "${description}" for ${address} timed out — lock did not wake within ${COMMAND_TIMEOUT_MS / 1000}s`);
                resolve(false);
            }, COMMAND_TIMEOUT_MS);

            this.commandQueue.set(address, { action, resolve, timer, description });
            console.log(`[Queue] Enqueued "${description}" for ${address} — touch keypad to wake lock`);
            this.emit("lockWaiting", lock);
        });
    }

    _cancelCommand(address, reason = "cancelled") {
        const pending = this.commandQueue.get(address);
        if (pending) {
            clearTimeout(pending.timer);
            console.log(`[Queue] "${pending.description}" for ${address} ${reason}`);
            pending.resolve(false);
            this.commandQueue.delete(address);
        }
    }

    async _drainQueue(address, lock) {
        const pending = this.commandQueue.get(address);
        if (!pending) return;
        this.commandQueue.delete(address);
        clearTimeout(pending.timer);
        console.log(`[Queue] Executing "${pending.description}" for ${address}`);
        try {
            const result = await pending.action(lock);
            pending.resolve(result);
        } catch (error) {
            console.error(`[Queue] Error in "${pending.description}" for ${address}:`, error);
            pending.resolve(false);
        }
    }

    // ---------------------------------------------------------------------------
    // Lock operations
    // ---------------------------------------------------------------------------

    async initLock(address) {
        const lock = this.newLocks.get(address);
        if (!lock) return false;
        if (!(await this._connectLock(lock))) return false;
        try {
            const res = await lock.initLock();
            if (res !== false) {
                this.pairedLocks.set(lock.getAddress(), lock);
                this.newLocks.delete(lock.getAddress());
                this._bindLockEvents(lock);
                this.emit("lockPaired", lock);
                return true;
            }
        } catch (error) {
            console.error(error);
        }
        return false;
    }

    async unlockLock(address) {
        return this._enqueueCommand(address, "unlock", lock => lock.unlock());
    }

    async lockLock(address) {
        return this._enqueueCommand(address, "lock", lock => lock.lock());
    }

    async setAutoLock(address, value) {
        return this._enqueueCommand(address, "setAutoLock", async (lock) => {
            const res = await lock.setAutoLockTime(value);
            this.emit("lockUpdated", lock);
            return res;
        });
    }

    async getCredentials(address) {
        return this._enqueueCommand(address, "getCredentials", async (lock) => {
            const [passcodes, cards, fingers] = await Promise.all([
                lock.hasPassCode() ? lock.getPassCodes() : Promise.resolve([]),
                lock.hasICCard() ? lock.getICCards() : Promise.resolve([]),
                lock.hasFingerprint() ? lock.getFingerprints() : Promise.resolve([])
            ]);
            for (const card of cards) card.alias = store.getCardAlias(card.cardNumber);
            for (const finger of fingers) finger.alias = store.getFingerAlias(finger.fpNumber);
            return { passcodes, cards, fingers };
        });
    }

    async addPasscode(address, type, passCode, startDate, endDate) {
        if (!this.pairedLocks.get(address)?.hasPassCode()) return false;
        return this._enqueueCommand(address, "addPasscode", lock => lock.addPassCode(type, passCode, startDate, endDate));
    }

    async updatePasscode(address, type, oldPasscode, newPasscode, startDate, endDate) {
        if (!this.pairedLocks.get(address)?.hasPassCode()) return false;
        return this._enqueueCommand(address, "updatePasscode", lock => lock.updatePassCode(type, oldPasscode, newPasscode, startDate, endDate));
    }

    async deletePasscode(address, type, passCode) {
        if (!this.pairedLocks.get(address)?.hasPassCode()) return false;
        return this._enqueueCommand(address, "deletePasscode", lock => lock.deletePassCode(type, passCode));
    }

    async getPasscodes(address) {
        if (!this.pairedLocks.get(address)?.hasPassCode()) return false;
        return this._enqueueCommand(address, "getPasscodes", lock => lock.getPassCodes());
    }

    async addCard(address, startDate, endDate, alias) {
        if (!this.pairedLocks.get(address)?.hasICCard()) return false;
        return this._enqueueCommand(address, "addCard", async (lock) => {
            const card = await lock.addICCard(startDate, endDate);
            store.setCardAlias(card, alias);
            return card;
        });
    }

    async updateCard(address, card, startDate, endDate, alias) {
        if (!this.pairedLocks.get(address)?.hasICCard()) return false;
        return this._enqueueCommand(address, "updateCard", async (lock) => {
            const result = await lock.updateICCard(card, startDate, endDate);
            store.setCardAlias(card, alias);
            return result;
        });
    }

    async deleteCard(address, card) {
        if (!this.pairedLocks.get(address)?.hasICCard()) return false;
        return this._enqueueCommand(address, "deleteCard", async (lock) => {
            const result = await lock.deleteICCard(card);
            store.deleteCardAlias(card);
            return result;
        });
    }

    async getCards(address) {
        if (!this.pairedLocks.get(address)?.hasICCard()) return false;
        return this._enqueueCommand(address, "getCards", async (lock) => {
            const cards = await lock.getICCards();
            for (const card of cards) card.alias = store.getCardAlias(card.cardNumber);
            return cards;
        });
    }

    async addFinger(address, startDate, endDate, alias) {
        if (!this.pairedLocks.get(address)?.hasFingerprint()) return false;
        return this._enqueueCommand(address, "addFinger", async (lock) => {
            const finger = await lock.addFingerprint(startDate, endDate);
            store.setFingerAlias(finger, alias);
            return finger;
        });
    }

    async updateFinger(address, finger, startDate, endDate, alias) {
        if (!this.pairedLocks.get(address)?.hasFingerprint()) return false;
        return this._enqueueCommand(address, "updateFinger", async (lock) => {
            const result = await lock.updateFingerprint(finger, startDate, endDate);
            store.setFingerAlias(finger, alias);
            return result;
        });
    }

    async deleteFinger(address, finger) {
        if (!this.pairedLocks.get(address)?.hasFingerprint()) return false;
        return this._enqueueCommand(address, "deleteFinger", async (lock) => {
            const result = await lock.deleteFingerprint(finger);
            store.deleteFingerAlias(finger);
            return result;
        });
    }

    async getFingers(address) {
        if (!this.pairedLocks.get(address)?.hasFingerprint()) return false;
        return this._enqueueCommand(address, "getFingers", async (lock) => {
            const fingers = await lock.getFingerprints();
            for (const f of fingers) f.alias = store.getFingerAlias(f.fpNumber);
            return fingers;
        });
    }

    async setAudio(address, audio) {
        if (!this.pairedLocks.get(address)?.hasLockSound()) return false;
        return this._enqueueCommand(address, "setAudio", async (lock) => {
            const sound = audio ? AudioManage.TURN_ON : AudioManage.TURN_OFF;
            const res = await lock.setLockSound(sound);
            this.emit("lockUpdated", lock);
            return res;
        });
    }

    async getOperationLog(address, reload = false) {
        return this._enqueueCommand(address, "getOperationLog", async (lock) => {
            let operations = JSON.parse(JSON.stringify(await lock.getOperationLog(true, reload)));
            const validOperations = [];
            for (const op of operations) {
                if (!op) continue;
                op.recordTypeName = LogOperateNames[op.recordType];
                if (LogOperateCategory.LOCK.includes(op.recordType)) op.recordTypeCategory = "LOCK";
                else if (LogOperateCategory.UNLOCK.includes(op.recordType)) op.recordTypeCategory = "UNLOCK";
                else if (LogOperateCategory.FAILED.includes(op.recordType)) op.recordTypeCategory = "FAILED";
                else op.recordTypeCategory = "OTHER";
                if (typeof op.password !== "undefined") {
                    if (LogOperateCategory.IC.includes(op.recordType)) op.passwordName = store.getCardAlias(op.password);
                    else if (LogOperateCategory.FR.includes(op.recordType)) op.passwordName = store.getFingerAlias(op.password);
                }
                validOperations.push(op);
            }
            return validOperations;
        });
    }

    async resetLock(address) {
        return this._enqueueCommand(address, "resetLock", async (lock) => {
            const res = await lock.resetLock();
            if (res) {
                lock.removeAllListeners();
                this.pairedLocks.delete(address);
                this.lockRssiMap.delete(address);
                this.emit("lockListChanged");
            }
            return res;
        });
    }

    // ---------------------------------------------------------------------------
    // Core connect — used internally when lock is known to be awake
    // ---------------------------------------------------------------------------

    async _connectLock(lock, readData = true) {
        if (lock.isConnected()) return true;

        const address = lock.getAddress();
        if (this._connecting.has(address)) {
            console.log(`[Manager] Connect already in progress for ${address}, skipping`);
            return false;
        }
        this._connecting.add(address);

        try {
            const ranked = this._getRankedProxies(address);
            if (ranked.length === 0) {
                console.error(`[Manager] No proxies available for lock ${address}`);
                return false;
            }

            for (const proxy of ranked) {
                const proxyLock = this._getProxyLock(proxy, address) || lock;
                try {
                    const rssi = this._getRssi(address, proxy.id);
                    console.log(`[Manager] Connecting to ${address} via [${proxy.id}] rssi=${rssi}`);
                    await sleep(300); // give adapter time to settle after monitor stop
                    const res = await proxyLock.connect(!readData);
                    if (res) {
                        if (proxyLock !== lock) this._transferCanonical(address, proxyLock);
                        console.log(`[Manager] Connected to ${address} via [${proxy.id}]`);
                        return true;
                    }
                    console.warn(`[Manager] Proxy [${proxy.id}] connect returned false for ${address}`);
                } catch (error) {
                    console.warn(`[Manager] Proxy [${proxy.id}] failed for ${address}: ${error.message}`);
                }
            }

            console.error(`[Manager] All proxies failed for lock ${address}`);
            return false;
        } finally {
            this._connecting.delete(address);
        }
    }

    _getRankedProxies(address) {
        const now = Date.now();
        const isAlive = (p) => now - p.lastSeen < PROXY_TIMEOUT_MS;
        const entries = (this.lockRssiMap.get(address) || [])
            .filter(e => now - e.lastSeen < RSSI_STALENESS_MS)
            .sort((a, b) => b.rssi - a.rssi);
        if (entries.length > 0) {
            const ranked = entries.map(e => this.proxies.find(p => p.id === e.proxyId)).filter(p => p && isAlive(p));
            if (ranked.length > 0) return ranked;
        }
        return this.proxies.filter(isAlive);
    }

    _getRssi(address, proxyId) {
        const entry = (this.lockRssiMap.get(address) || []).find(e => e.proxyId === proxyId);
        return entry ? entry.rssi : null;
    }

    _getProxyLock(proxy, address) {
        const client = proxy.client;
        if (typeof client.getLocks === "function") {
            return client.getLocks().find(l => l.getAddress() === address) || null;
        }
        if (client.pairedLocks instanceof Map) return client.pairedLocks.get(address) || null;
        return null;
    }

    _transferCanonical(address, newLock) {
        const old = this.pairedLocks.get(address);
        if (old) {
            old.removeAllListeners();
            this._bindLockEvents(newLock);
            this.pairedLocks.set(address, newLock);
        }
    }

    // ---------------------------------------------------------------------------
    // RSSI tracking
    // ---------------------------------------------------------------------------

    _updateRssi(proxyId, address, rssi) {
        if (!this.lockRssiMap.has(address)) this.lockRssiMap.set(address, []);
        const entries = this.lockRssiMap.get(address);
        const existing = entries.find(e => e.proxyId === proxyId);
        if (existing) { existing.rssi = rssi; existing.lastSeen = Date.now(); }
        else entries.push({ proxyId, rssi, lastSeen: Date.now() });
    }

    _pruneStaleRssi() {
        const now = Date.now();
        for (const [addr, entries] of this.lockRssiMap) {
            const fresh = entries.filter(e => now - e.lastSeen < RSSI_STALENESS_MS);
            if (fresh.length === 0) this.lockRssiMap.delete(addr);
            else this.lockRssiMap.set(addr, fresh);
        }
    }

    /**
     * Get the battery level from a lock object, trying multiple API shapes
     * since different SDK versions expose it differently.
     * @param {import('ttlock-sdk-js').TTLock} lock
     * @returns {number|null}
     */
    _getBattery(lock) {
        if (typeof lock.getBattery === "function") {
            const b = lock.getBattery();
            if (b !== null && b !== undefined && b >= 0) return b;
        }
        if (typeof lock.batteryCapacity === "number" && lock.batteryCapacity >= 0) {
            return lock.batteryCapacity;
        }
        return null;
    }

    // ---------------------------------------------------------------------------
    // Scanner / monitor events
    // ---------------------------------------------------------------------------

    async _onScanStarted() {
        this.scanning = true;
        console.log("BLE Scan started");
        this.emit("scanStart");
    }

    async _onScanStopped() {
        this.scanning = false;
        console.log("BLE Scan stopped");
        this.emit("scanStop");
        if (this.newLocks.size === 0) {
            setTimeout(() => this.proxies.forEach(p => p.client.startMonitor()), 500);
        } else {
            console.log("New locks pending pairing — not restarting monitor");
        }
    }

    /**
     * Called when any proxy finds a lock during scan or monitor.
     *
     * Key responsibilities:
     * 1. Update battery passively from advertisement data (no connection needed)
     * 2. If lock has a pending command, connect and execute it
     * 3. If lock has new events (used physically), connect and read log
     *
     * @param {import('ttlock-sdk-js').TTLock} lock
     * @param {string} proxyId
     */
    async _onFoundLock(lock, proxyId) {
        const address = lock.getAddress();
        let listChanged = false;

        if (lock.isPaired()) {
            const isNewToPool = !this.pairedLocks.has(address);

            if (isNewToPool) {
                // First sighting of this paired lock — add to shared pool
                this.pairedLocks.set(address, lock);
                this._bindLockEvents(lock);
                console.log(`Discovered paired lock: ${address} via [${proxyId}] rssi=${lock.rssi}`);
                listChanged = true;
                // Emit lockDiscovered so HA can configure the entity immediately,
                // without waiting for a BLE connection
                this.emit("lockDiscovered", lock);
            }

            // --- Passive battery update from advertisement ---
            // Battery is encoded in the manufacturer data and decoded by the SDK.
            // We get a free update on every advertisement without connecting.
            const canonical = this.pairedLocks.get(address);
            const battery = this._getBattery(lock);
            if (battery !== null) {
                // Propagate to canonical lock object if this came from a different proxy
                if (lock !== canonical) {
                    canonical.batteryCapacity = battery;
                }
                console.log(`Battery update for ${address}: ${battery}% (via [${proxyId}])`);
                this.emit("lockBatteryUpdated", canonical);
            }

            // --- Command queue / event processing ---
            if (!this._connecting.has(address)) {
                const hasPendingCommand = this.commandQueue.has(address);
                const hasNewEvents = lock.hasNewEvents && lock.hasNewEvents();

                if (hasPendingCommand || hasNewEvents) {
                    const reason = hasPendingCommand ? "pending command" : "new events";
                    console.log(`Lock ${address} is awake (rssi=${lock.rssi}), processing ${reason}`);

                    // Stop monitor on all proxies before connecting
                    for (const p of this.proxies) {
                        try { await p.client.stopMonitor(); } catch (_) { }
                    }
                    await sleep(300);

                    const connected = await this._connectLock(canonical);
                    if (connected) {
                        const currentLock = this.pairedLocks.get(address);
                        if (hasPendingCommand) {
                            await this._drainQueue(address, currentLock);
                        }
                        if (hasNewEvents) {
                            await this._processOperationLog(currentLock);
                        }
                        await currentLock.disconnect();
                    } else {
                        console.warn(`Connect failed for ${address} despite lock being awake`);
                    }

                    // Restart monitor after command
                    if (this.newLocks.size === 0) {
                        setTimeout(() => this.proxies.forEach(p => p.client.startMonitor()), 500);
                    }
                }
            }

        } else if (!lock.isInitialized()) {
            if (!this.newLocks.has(address)) {
                console.log(`Discovered new lock: ${address} via [${proxyId}] rssi=${lock.rssi}`);
                this.newLocks.set(address, lock);
                listChanged = true;
                // Stop everything so the lock stays connectable for pairing
                for (const p of this.proxies) {
                    try { await p.client.stopScanLock(); } catch (_) { }
                    try { await p.client.stopMonitor(); } catch (_) { }
                }
                console.log("New lock found — all scanning stopped, waiting for user to pair");
            }
        } else {
            try {
                console.log("Discovered unknown lock:", lock.toJSON());
            } catch (e) {
                console.log("Discovered unknown lock:", address, "(toJSON circular ref)");
            }
        }

        if (listChanged) this.emit("lockListChanged");
    }

    /**
     * Save lock data — flat array, same format as original.
     * All proxy clients share the same lock data so we only need one client's copy.
     */
    async _onUpdatedLockData() {
        if (this.proxies.length === 0) return;
        const lockData = this.proxies[0].client.getLockData();
        store.setLockData(lockData);
        // Keep all other proxy clients in sync
        for (let i = 1; i < this.proxies.length; i++) {
            this.proxies[i].client.setLockData(lockData);
        }
    }

    _bindLockEvents(lock) {
        lock.on("connected", this._onLockConnected.bind(this));
        lock.on("disconnected", this._onLockDisconnected.bind(this));
        lock.on("locked", this._onLockLocked.bind(this));
        lock.on("unlocked", this._onLockUnlocked.bind(this));
        lock.on("updated", this._onLockUpdated.bind(this));
        lock.on("scanICStart", () => this.emit("lockCardScan", lock));
        lock.on("scanFRStart", () => this.emit("lockFingerScan", lock));
        lock.on("scanFRProgress", () => this.emit("lockFingerScanProgress", lock));
    }

    async _onLockConnected(lock) {
        if (lock.isPaired()) {
            this.pairedLocks.set(lock.getAddress(), lock);
            console.log("Connected to paired lock", lock.getAddress());
            this.emit("lockConnected", lock);
        } else {
            console.log("Connected to new lock", lock.getAddress());
        }
    }

    async _onLockDisconnected(lock) {
        console.log("Disconnected from lock", lock.getAddress());
        if (lock.isPaired() && this.newLocks.size === 0) {
            setTimeout(() => this.proxies.forEach(p => p.client.startMonitor()), 500);
        }
    }

    async _onLockLocked(lock) { this.emit("lockLock", lock); }
    async _onLockUnlocked(lock) { this.emit("lockUnlock", lock); }

    async _onLockUpdated(lock, paramsChanged) {
        console.log("lockUpdated", paramsChanged);
        if (paramsChanged.newEvents === true && lock.hasNewEvents()) {
            if (!lock.isConnected()) await this._connectLock(lock);
            await this._processOperationLog(lock);
        }
        if (paramsChanged.lockedStatus === true) {
            const status = await lock.getLockStatus();
            if (status === LockedStatus.LOCKED) {
                console.log(">>>>>> Lock is now locked from new event <<<<<<");
                this.emit("lockLock", lock);
            }
        }
        if (paramsChanged.batteryCapacity === true) {
            // Battery changed during a connected session — emit both events
            this.emit("lockBatteryUpdated", lock);
            this.emit("lockUpdated", lock);
        }
        await lock.disconnect();
    }

    async _processOperationLog(lock) {
        const operations = await lock.getOperationLog();
        let lastStatus = LockedStatus.UNKNOWN;
        for (const op of operations) {
            if (LogOperateCategory.UNLOCK.includes(op.recordType)) {
                lastStatus = LockedStatus.UNLOCKED;
                console.log(">>>>>> Lock was unlocked <<<<<<");
                this.emit("lockUnlock", lock);
            } else if (LogOperateCategory.LOCK.includes(op.recordType)) {
                lastStatus = LockedStatus.LOCKED;
                console.log(">>>>>> Lock was locked <<<<<<");
                this.emit("lockLock", lock);
            }
        }
        const status = await lock.getLockStatus();
        if (lastStatus !== LockedStatus.UNKNOWN && status !== lastStatus) {
            if (status === LockedStatus.LOCKED) {
                console.log(">>>>>> Lock is now locked <<<<<<");
                this.emit("lockLock", lock);
            } else if (status === LockedStatus.UNLOCKED) {
                console.log(">>>>>> Lock is now unlocked <<<<<<");
                this.emit("lockUnlock", lock);
            }
        }
    }

    async _scanTimer() {
        if (!this.scanTimer) {
            this.scanTimer = setTimeout(() => this.stopScan(), 30 * 1000);
        }
    }
}

const manager = new Manager();
module.exports = manager;