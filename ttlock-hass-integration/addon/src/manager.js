'use strict';

const EventEmitter = require('events');
const store = require("./store");
const { TTLockClient, AudioManage, LockedStatus, LogOperateCategory, LogOperateNames } = require("ttlock-sdk-js");

const RSSI_STALENESS_MS = 120_000; // drop RSSI reading after 2 minutes
const PROXY_TIMEOUT_MS = 60_000; // proxy considered dead after 60s no activity

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
 * - ONE shared flat lock list (pairedLocks / newLocks) — same format as original,
 *   saved/loaded to lockData.json exactly as before.
 *
 * - Each TTLockClient (proxy) gets the full lock list on startup so it can
 *   recognise and decrypt advertisements from any lock.
 *
 * - An ephemeral RSSI table (never saved) tracks signal strength per lock per proxy.
 *   Updated whenever any proxy sees a lock advertisement.
 *
 * - When a command needs to be sent to a lock, the proxy with the strongest
 *   recent RSSI for that lock is chosen. If it fails, the next-best is tried.
 *
 * - Lock objects: the SDK binds a TTLock object to the scanner that created it,
 *   so each proxy maintains its own TTLock instances internally. We keep ONE
 *   canonical TTLock per address in pairedLocks (the one from the first proxy
 *   that found it) for event binding and state, but when connecting we ask the
 *   best-proxy's client for its version of the lock.
 *
 * Events:
 * - lockListChanged, lockPaired, lockConnected, lockLock, lockUnlock,
 *   lockUpdated, scanStart, scanStop
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
         * Canonical paired lock objects — ONE per address, same as original.
         * Saved/loaded via store exactly as before.
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
         * Updated on every advertisement from every proxy.
         * @type {Map<string, Array<{ proxyId: string, rssi: number, lastSeen: number }>>}
         */
        this.lockRssiMap = new Map();

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

                // Support per-proxy AES keys: gateway_key can be comma-separated.
                // If fewer keys than hosts, the last key is reused.
                const keys = this.gateway_key.split(',').map(k => k.trim()).filter(Boolean);
                const keyFor = (i) => keys[i] || keys[keys.length - 1] || "";

                const ports = this.gateway_port.split(',').map(k => k.trim());
                const portFor = (i) => ports[i] || ports[ports.length - 1] || "";

                // The shared lock data from store — fed to every proxy client
                const lockData = store.getLockData();

                for (let i = 0; i < hosts.length; i++) {
                    const host = hosts[i];
                    let clientOptions = {};
                    if (this.gateway === "noble") {
                        clientOptions.scannerType = "noble-websocket";
                        clientOptions.scannerOptions = {
                            websocketHost: host,
                            websocketPort: portFor(i),
                            websocketAesKey: keyFor(i),
                            websocketUsername: this.gateway_user,
                            websocketPassword: this.gateway_pass
                        };
                    }

                    const proxyId = host;
                    const client = new TTLockClient(clientOptions);
                    const proxy = { id: proxyId, client, lastSeen: Date.now() };
                    this.proxies.push(proxy);

                    // Give every proxy the full shared lock list
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

    /**
     * Push the shared lock list to all proxy clients.
     * Called after store is updated externally (e.g. config import).
     */
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

    /** Proxy/RSSI debug info */
    getProxyStatus() {
        const now = Date.now();
        const proxies = this.proxies.map(p => ({
            id: p.id,
            alive: now - p.lastSeen < PROXY_TIMEOUT_MS,
            lastSeenMs: now - p.lastSeen
        }));
        const locks = {};
        for (const [addr, entries] of this.lockRssiMap) {
            locks[addr] = entries.map(e => ({
                proxyId: e.proxyId,
                rssi: e.rssi,
                staleMs: now - e.lastSeen,
                fresh: now - e.lastSeen < RSSI_STALENESS_MS
            }));
        }
        return { proxies, locks };
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
        return this._withLock(address, lock => lock.unlock());
    }

    async lockLock(address) {
        return this._withLock(address, lock => lock.lock());
    }

    async setAutoLock(address, value) {
        return this._withLock(address, async (lock) => {
            const res = await lock.setAutoLockTime(value);
            this.emit("lockUpdated", lock);
            return res;
        });
    }

    async getCredentials(address) {
        const [passcodes, cards, fingers] = await Promise.all([
            this.getPasscodes(address),
            this.getCards(address),
            this.getFingers(address)
        ]);
        return { passcodes, cards, fingers };
    }

    async addPasscode(address, type, passCode, startDate, endDate) {
        if (!this.pairedLocks.get(address)?.hasPassCode()) return false;
        return this._withLock(address, lock => lock.addPassCode(type, passCode, startDate, endDate));
    }

    async updatePasscode(address, type, oldPasscode, newPasscode, startDate, endDate) {
        if (!this.pairedLocks.get(address)?.hasPassCode()) return false;
        return this._withLock(address, lock => lock.updatePassCode(type, oldPasscode, newPasscode, startDate, endDate));
    }

    async deletePasscode(address, type, passCode) {
        if (!this.pairedLocks.get(address)?.hasPassCode()) return false;
        return this._withLock(address, lock => lock.deletePassCode(type, passCode));
    }

    async getPasscodes(address) {
        if (!this.pairedLocks.get(address)?.hasPassCode()) return false;
        return this._withLock(address, lock => lock.getPassCodes());
    }

    async addCard(address, startDate, endDate, alias) {
        if (!this.pairedLocks.get(address)?.hasICCard()) return false;
        return this._withLock(address, async (lock) => {
            const card = await lock.addICCard(startDate, endDate);
            store.setCardAlias(card, alias);
            return card;
        });
    }

    async updateCard(address, card, startDate, endDate, alias) {
        if (!this.pairedLocks.get(address)?.hasICCard()) return false;
        return this._withLock(address, async (lock) => {
            const result = await lock.updateICCard(card, startDate, endDate);
            store.setCardAlias(card, alias);
            return result;
        });
    }

    async deleteCard(address, card) {
        if (!this.pairedLocks.get(address)?.hasICCard()) return false;
        return this._withLock(address, async (lock) => {
            const result = await lock.deleteICCard(card);
            store.deleteCardAlias(card);
            return result;
        });
    }

    async getCards(address) {
        if (!this.pairedLocks.get(address)?.hasICCard()) return false;
        return this._withLock(address, async (lock) => {
            const cards = await lock.getICCards();
            for (const card of cards) card.alias = store.getCardAlias(card.cardNumber);
            return cards;
        });
    }

    async addFinger(address, startDate, endDate, alias) {
        if (!this.pairedLocks.get(address)?.hasFingerprint()) return false;
        return this._withLock(address, async (lock) => {
            const finger = await lock.addFingerprint(startDate, endDate);
            store.setFingerAlias(finger, alias);
            return finger;
        });
    }

    async updateFinger(address, finger, startDate, endDate, alias) {
        if (!this.pairedLocks.get(address)?.hasFingerprint()) return false;
        return this._withLock(address, async (lock) => {
            const result = await lock.updateFingerprint(finger, startDate, endDate);
            store.setFingerAlias(finger, alias);
            return result;
        });
    }

    async deleteFinger(address, finger) {
        if (!this.pairedLocks.get(address)?.hasFingerprint()) return false;
        return this._withLock(address, async (lock) => {
            const result = await lock.deleteFingerprint(finger);
            store.deleteFingerAlias(finger);
            return result;
        });
    }

    async getFingers(address) {
        if (!this.pairedLocks.get(address)?.hasFingerprint()) return false;
        return this._withLock(address, async (lock) => {
            const fingers = await lock.getFingerprints();
            for (const f of fingers) f.alias = store.getFingerAlias(f.fpNumber);
            return fingers;
        });
    }

    async setAudio(address, audio) {
        if (!this.pairedLocks.get(address)?.hasLockSound()) return false;
        return this._withLock(address, async (lock) => {
            const sound = audio ? AudioManage.TURN_ON : AudioManage.TURN_OFF;
            const res = await lock.setLockSound(sound);
            this.emit("lockUpdated", lock);
            return res;
        });
    }

    async getOperationLog(address, reload = false) {
        const lock = this.pairedLocks.get(address);
        if (!lock) return false;
        if (!(await this._connectLock(lock))) return false;
        try {
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
        } catch (error) {
            console.error(error);
            return false;
        }
    }

    async resetLock(address) {
        const lock = this.pairedLocks.get(address);
        if (!lock) return false;
        if (!(await this._connectLock(lock))) return false;
        try {
            const res = await lock.resetLock();
            if (res) {
                lock.removeAllListeners();
                this.pairedLocks.delete(address);
                this.lockRssiMap.delete(address);
                this.emit("lockListChanged");
            }
            return res;
        } catch (error) {
            console.error(error);
        }
        return false;
    }

    // ---------------------------------------------------------------------------
    // Core routing — connect via best proxy by RSSI
    // ---------------------------------------------------------------------------

    /**
     * Connect to a lock using the proxy with the strongest recent RSSI.
     * Falls back through ranked proxies until one succeeds.
     *
     * Each proxy client has its own internal TTLock instance for the same
     * physical lock (because the SDK binds lock objects to their scanner).
     * We ask each proxy's client for its version of the lock and attempt
     * to connect through it.
     *
     * @param {import('ttlock-sdk-js').TTLock} lock  canonical lock object
     * @param {boolean} readData
     * @returns {Promise<boolean>}
     */
    async _connectLock(lock, readData = true) {
        if (this.scanning) return false;
        if (lock.isConnected()) return true;

        const address = lock.getAddress();

        // Prevent concurrent connect attempts for the same lock
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
                // Get this proxy's internal lock object for this address
                const proxyLock = this._getProxyLock(proxy, address) || lock;

                try {
                    const rssi = this._getRssi(address, proxy.id);
                    console.log(`[Manager] Attempting connect to ${address} via [${proxy.id}] rssi=${rssi}`);
                    const res = await proxyLock.connect(!readData);
                    if (res) {
                        // If we connected via a different proxy's lock object, update
                        // our canonical reference so subsequent calls use the right object
                        if (proxyLock !== lock) {
                            this._transferCanonical(address, proxyLock);
                        }
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

    /**
     * Returns proxies sorted by RSSI for this lock address, best first.
     * Falls back to all alive proxies if no fresh RSSI data exists.
     */
    _getRankedProxies(address) {
        const now = Date.now();
        const isAlive = (p) => now - p.lastSeen < PROXY_TIMEOUT_MS;

        const entries = (this.lockRssiMap.get(address) || [])
            .filter(e => now - e.lastSeen < RSSI_STALENESS_MS)
            .sort((a, b) => b.rssi - a.rssi);

        if (entries.length > 0) {
            const ranked = entries
                .map(e => this.proxies.find(p => p.id === e.proxyId))
                .filter(p => p && isAlive(p));
            if (ranked.length > 0) return ranked;
        }

        // No fresh RSSI data — return all alive proxies
        return this.proxies.filter(isAlive);
    }

    /** Get the RSSI a proxy last reported for a lock, or null */
    _getRssi(address, proxyId) {
        const entry = (this.lockRssiMap.get(address) || []).find(e => e.proxyId === proxyId);
        return entry ? entry.rssi : null;
    }

    /**
     * Ask a proxy's TTLockClient for its internal lock object for this address.
     * Each client maintains its own TTLock instances bound to its scanner.
     * Returns null if the proxy hasn't seen this lock yet.
     */
    _getProxyLock(proxy, address) {
        const client = proxy.client;
        // Try the public API first
        if (typeof client.getLocks === "function") {
            const locks = client.getLocks();
            return locks.find(l => l.getAddress() === address) || null;
        }
        // Fallback: internal map (may vary by SDK version)
        if (client.pairedLocks instanceof Map) {
            return client.pairedLocks.get(address) || null;
        }
        return null;
    }

    /**
     * When a different proxy's lock object successfully connected, update our
     * canonical pairedLocks reference and rebind events to the new object.
     */
    _transferCanonical(address, newLock) {
        const old = this.pairedLocks.get(address);
        if (old) {
            old.removeAllListeners();
            this._bindLockEvents(newLock);
            this.pairedLocks.set(address, newLock);
            console.log(`[Manager] Canonical lock object updated for ${address}`);
        }
    }

    /**
     * Generic helper: get lock, connect, run action, return result.
     * Re-fetches canonical lock after connect in case _transferCanonical ran.
     */
    async _withLock(address, action) {
        const lock = this.pairedLocks.get(address);
        if (!lock) return false;
        if (!(await this._connectLock(lock))) return false;
        try {
            return await action(this.pairedLocks.get(address)); // re-fetch in case transferred
        } catch (error) {
            console.error(error);
            return false;
        }
    }

    // ---------------------------------------------------------------------------
    // RSSI tracking (ephemeral — never persisted)
    // ---------------------------------------------------------------------------

    _updateRssi(proxyId, address, rssi) {
        if (!this.lockRssiMap.has(address)) this.lockRssiMap.set(address, []);
        const entries = this.lockRssiMap.get(address);
        const existing = entries.find(e => e.proxyId === proxyId);
        if (existing) {
            existing.rssi = rssi;
            existing.lastSeen = Date.now();
        } else {
            entries.push({ proxyId, rssi, lastSeen: Date.now() });
        }
    }

    _pruneStaleRssi() {
        const now = Date.now();
        for (const [addr, entries] of this.lockRssiMap) {
            const fresh = entries.filter(e => now - e.lastSeen < RSSI_STALENESS_MS);
            if (fresh.length === 0) this.lockRssiMap.delete(addr);
            else this.lockRssiMap.set(addr, fresh);
        }
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
        console.log("BLE Scan stopped — refreshing paired locks");
        for (const address of this.connectQueue) {
            if (this.pairedLocks.has(address)) {
                const lock = this.pairedLocks.get(address);
                console.log("Auto connect to", address);
                const result = await lock.connect();
                if (result === true) {
                    await lock.disconnect();
                    console.log("Successful connect attempt to paired lock", address);
                    this.connectQueue.delete(address);
                } else {
                    console.log("Unsuccessful connect attempt to paired lock", address);
                }
            }
        }
        this.emit("scanStop");
        // Don't restart monitor if new locks are waiting to be paired
        if (this.newLocks.size === 0) {
            setTimeout(() => this.proxies.forEach(p => p.client.startMonitor()), 200);
        } else {
            console.log("New locks pending pairing — not restarting monitor");
        }
    }

    /**
     * Called when any proxy finds a lock during scan or monitor.
     * RSSI has already been recorded before this is called (in init()).
     *
     * @param {import('ttlock-sdk-js').TTLock} lock  - the proxy's lock object
     * @param {string} proxyId
     */
    async _onFoundLock(lock, proxyId) {
        const address = lock.getAddress();
        let listChanged = false;

        if (lock.isPaired()) {
            if (!this.pairedLocks.has(address)) {
                // First sighting — add to shared pool and bind events
                this.pairedLocks.set(address, lock);
                this._bindLockEvents(lock);
                console.log(`Discovered paired lock: ${address} via [${proxyId}] rssi=${lock.rssi}`);

                const anyMonitoring = this.proxies.some(p => p.client.isMonitoring());
                if (anyMonitoring) {
                    const result = await lock.connect();
                    if (result === true) {
                        console.log("Successful connect attempt to paired lock", address);
                        await this._processOperationLog(lock);
                    } else {
                        console.log("Unsuccessful connect attempt to paired lock", address);
                        this.connectQueue.add(address);
                    }
                    await lock.disconnect();
                } else {
                    this.connectQueue.add(address);
                }
                listChanged = true;
            } else {
                // Already known — RSSI already updated, nothing else to do
                if (proxyId !== this._getBestProxyId(address)) {
                    console.log(`Lock ${address} also seen by [${proxyId}] rssi=${lock.rssi}`);
                }
            }

        } else if (!lock.isInitialized()) {
            if (!this.newLocks.has(address)) {
                console.log(`Discovered new lock: ${address} via [${proxyId}] rssi=${lock.rssi}`);
                this.newLocks.set(address, lock);
                listChanged = true;
                // Stop all scanning/monitoring so the lock stays connectable for pairing
                for (const p of this.proxies) {
                    try { await p.client.stopScanLock(); } catch (_) { }
                    try { await p.client.stopMonitor(); } catch (_) { }
                }
                console.log("New lock found — all scanning stopped, waiting for user to pair");
            }
            // Already in newLocks — do nothing, wait for user action
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
     * Deduplication ensures no duplicates if multiple proxies report the same lock.
     */
    async _onUpdatedLockData() {
        if (this.proxies.length === 0) return;
        // Use the first proxy's lock data as the canonical source — all proxies
        // should have the same data since they share the same setLockData() calls.
        const lockData = this.proxies[0].client.getLockData();
        store.setLockData(lockData);
        // Keep all other proxy clients in sync
        for (let i = 1; i < this.proxies.length; i++) {
            this.proxies[i].client.setLockData(lockData);
        }
    }

    /** Returns the proxyId with the best current RSSI for a lock */
    _getBestProxyId(address) {
        const entries = (this.lockRssiMap.get(address) || [])
            .filter(e => Date.now() - e.lastSeen < RSSI_STALENESS_MS)
            .sort((a, b) => b.rssi - a.rssi);
        return entries.length > 0 ? entries[0].proxyId : null;
    }

    // ---------------------------------------------------------------------------
    // Lock events
    // ---------------------------------------------------------------------------

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
        // Only resume monitoring if no new locks are waiting to be paired
        if (lock.isPaired() && this.newLocks.size === 0) {
            this.proxies.forEach(p => p.client.startMonitor());
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