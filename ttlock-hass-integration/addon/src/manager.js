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
 * Events:
 * - lockListChanged     - a lock was found / list changed
 * - lockPaired          - a lock was paired
 * - lockConnected       - a connection to a lock was established
 * - lockLock            - a lock was locked
 * - lockUnlock          - a lock was unlocked
 * - scanStart           - scanning has started
 * - scanStop            - scanning has stopped
 */
class Manager extends EventEmitter {
    constructor() {
        super();
        this.startupStatus = -1;

        /**
         * Array of proxy entries, one per gateway_host value.
         * Each entry: { id: string, client: TTLockClient, lastSeen: number }
         * @type {Array<{ id: string, client: TTLockClient, lastSeen: number }>}
         */
        this.proxies = [];

        this.scanning = false;
        /** @type {NodeJS.Timeout} */
        this.scanTimer = undefined;

        /**
         * Paired locks visible during scan.
         * @type {Map<string, import('ttlock-sdk-js').TTLock>}
         */
        this.pairedLocks = new Map();

        /**
         * New (unpaired) locks visible during scan.
         * @type {Map<string, import('ttlock-sdk-js').TTLock>}
         */
        this.newLocks = new Map();

        /**
         * Locks we need to connect to at least once after scan stops.
         * @type {Set<string>}
         */
        this.connectQueue = new Set();

        /**
         * RSSI table: lockAddress -> Array<{ proxyId, rssi, lastSeen }>
         * Updated every time any proxy advertises a lock.
         * @type {Map<string, Array<{ proxyId: string, rssi: number, lastSeen: number }>>}
         */
        this.lockRssiMap = new Map();

        /**
         * Maps lockAddress -> proxyId of the proxy that currently owns this lock object.
         * A lock object is bound to the scanner that found it, so commands must go
         * through that same client. This lets us find the right client for a given lock.
         * @type {Map<string, string>}
         */
        this.lockOwnerMap = new Map();

        /** @type {'none'|'noble'} */
        this.gateway = 'none';
        this.gateway_host = "";
        this.gateway_port = 0;
        this.gateway_key = "";
        this.gateway_user = "";
        this.gateway_pass = "";

        // Periodically prune stale RSSI entries
        this._pruneInterval = setInterval(() => this._pruneStaleRssi(), 30_000);
        this._connecting = new Set();
    }

    async init() {
        if (this.proxies.length === 0) {
            try {
                const hosts = this.gateway === "noble"
                    ? this.gateway_host.split(',').map(h => h.trim()).filter(Boolean)
                    : ["local"];

                const keys = this.gateway_key.split(',').map(k => k.trim());
                const ports = this.gateway_port.split(',').map(k => k.trim());
                for (let i = 0; i < hosts.length; i++) {
                    const host = hosts[i];
                    const key = keys[i] || keys[0] || this.gateway_key; // fallback to first key if not enough keys
                    const port = ports[i] || ports[0] || this.gateway_key; // fallback to first port if not enough keys

                    let clientOptions = {};
                    if (this.gateway === "noble") {
                        clientOptions.scannerType = "noble-websocket";
                        clientOptions.scannerOptions = {
                            websocketHost: host,
                            websocketPort: port,
                            websocketAesKey: key,          // <-- per-proxy key
                            websocketUsername: this.gateway_user,
                            websocketPassword: this.gateway_pass
                        };
                    }

                    const proxyId = host;
                    const client = new TTLockClient(clientOptions);
                    const proxy = { id: proxyId, client, lastSeen: Date.now() };
                    this.proxies.push(proxy);

                    client.setLockData(store.getLockData());

                    client.on("ready", () => {
                        proxy.lastSeen = Date.now();
                        client.startMonitor();
                    });

                    // Capture proxyId in closure — critical so each client knows who it is
                    client.on("foundLock", (lock) => {
                        proxy.lastSeen = Date.now();
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

    /** Returns proxy/RSSI status per lock — useful for debugging and the UI */
    getProxyStatus() {
        const now = Date.now();
        const proxies = this.proxies.map(p => ({
            id: p.id,
            alive: now - p.lastSeen < PROXY_TIMEOUT_MS,
            lastSeenMs: now - p.lastSeen
        }));
        const locks = {};
        for (const [addr, entries] of this.lockRssiMap) {
            locks[addr] = {
                ownerProxy: this.lockOwnerMap.get(addr),
                seenBy: entries.map(e => ({
                    proxyId: e.proxyId,
                    rssi: e.rssi,
                    staleMs: now - e.lastSeen,
                    fresh: now - e.lastSeen < RSSI_STALENESS_MS
                }))
            };
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
            const res = await p.client.startScanLock();
            if (res) anyStarted = true;
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
            const res = await p.client.stopScanLock();
            if (res) anyStopped = true;
        }
        return anyStopped;
    }

    // ---------------------------------------------------------------------------
    // Lock operations — all go through _withLock -> _connectLock
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
                this.lockOwnerMap.delete(address);
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
    // Core routing
    // ---------------------------------------------------------------------------

    /**
     * Connect to a lock through the best available proxy.
     *
     * Key insight: a TTLock object is internally bound to the TTLockClient
     * scanner that discovered it — you cannot freely swap scanners on an
     * existing lock object. The strategy therefore is:
     *
     * 1. Build a ranked list of proxies for this lock (by RSSI, best first).
     * 2. For each proxy in order:
     *    a. If it's the owning proxy, use the existing lock object directly.
     *    b. If it's a different proxy, try to get that proxy's version of the
     *       lock (it may have seen the same physical device in monitor mode),
     *       then attempt to connect through it and migrate ownership if it works.
     * 3. First successful connect wins. If all fail, return false.
     *
     * @param {import('ttlock-sdk-js').TTLock} lock
     * @param {boolean} readData
     * @returns {Promise<boolean>}
     */
    async _connectLock(lock, readData = true) {
        if (this.scanning) return false;
        if (lock.isConnected()) return true;

        const address = lock.getAddress();

        // Prevent concurrent connect attempts to the same lock
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
                const lockForProxy = this._getLockForProxy(address, proxy.id);
                if (!lockForProxy) {
                    console.warn(`[Manager] Proxy [${proxy.id}] has no lock object for ${address}, skipping`);
                    continue;
                }
                try {
                    const rssi = this._getRssi(address, proxy.id);
                    console.log(`[Manager] Attempting connect to ${address} via [${proxy.id}] rssi=${rssi}`);
                    const res = await lockForProxy.connect(!readData);
                    if (res) {
                        if (lockForProxy !== lock) {
                            console.log(`[Manager] Migrating ${address} ownership to [${proxy.id}]`);
                            this._migrateOwnership(address, lockForProxy, proxy.id);
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
     * Returns proxies sorted by RSSI for a lock address, best first.
     * Falls back to all alive proxies (owner first) if no RSSI data exists.
     * @param {string} address
     * @returns {Array<{ id: string, client: TTLockClient, lastSeen: number }>}
     */
    _getRankedProxies(address) {
        const now = Date.now();
        const isAlive = (p) => now - p.lastSeen < PROXY_TIMEOUT_MS;

        const rssiEntries = (this.lockRssiMap.get(address) || [])
            .filter(e => now - e.lastSeen < RSSI_STALENESS_MS)
            .sort((a, b) => b.rssi - a.rssi);

        if (rssiEntries.length > 0) {
            const ranked = rssiEntries
                .map(e => this.proxies.find(p => p.id === e.proxyId))
                .filter(p => p && isAlive(p));
            if (ranked.length > 0) return ranked;
        }

        // No fresh RSSI data — all alive proxies, owner first
        const ownerProxyId = this.lockOwnerMap.get(address);
        return this.proxies
            .filter(isAlive)
            .sort((a, b) => {
                if (a.id === ownerProxyId) return -1;
                if (b.id === ownerProxyId) return 1;
                return 0;
            });
    }

    /**
     * Get the RSSI value a proxy last reported for a lock.
     */
    _getRssi(address, proxyId) {
        const entries = this.lockRssiMap.get(address) || [];
        const entry = entries.find(e => e.proxyId === proxyId);
        return entry ? entry.rssi : null;
    }

    /**
     * Get the lock object that belongs to a specific proxy's scanner.
     *
     * The owning proxy has the object in pairedLocks/newLocks.
     * A non-owning proxy may also have a lock object if it saw the same device
     * during monitor mode — we retrieve it from that client's internal state.
     *
     * @param {string} address
     * @param {string} proxyId
     * @returns {import('ttlock-sdk-js').TTLock|null}
     */
    _getLockForProxy(address, proxyId) {
        const ownerProxyId = this.lockOwnerMap.get(address);

        // Owning proxy — return the lock object we already track
        if (proxyId === ownerProxyId) {
            return this.pairedLocks.get(address) || this.newLocks.get(address) || null;
        }

        // Non-owning proxy — ask its TTLockClient for any lock it has seen
        const proxy = this.proxies.find(p => p.id === proxyId);
        if (!proxy) return null;

        // TTLockClient API: try common shapes for accessing its internal lock list
        if (typeof proxy.client.getLocks === "function") {
            const locks = proxy.client.getLocks();
            return locks.find(l => l.getAddress() === address) || null;
        }
        if (proxy.client.pairedLocks instanceof Map) {
            return proxy.client.pairedLocks.get(address) || null;
        }

        return null;
    }

    /**
     * Transfer ownership of a lock to a new proxy after a successful connect
     * through a non-owning proxy.
     * @param {string} address
     * @param {import('ttlock-sdk-js').TTLock} newLock  - the new proxy's lock object
     * @param {string} newProxyId
     */
    _migrateOwnership(address, newLock, newProxyId) {
        const oldLock = this.pairedLocks.get(address);
        if (oldLock) {
            oldLock.removeAllListeners();
            this._bindLockEvents(newLock);
            this.pairedLocks.set(address, newLock);
        }
        this.lockOwnerMap.set(address, newProxyId);
    }

    /**
     * Generic helper: connect + run action + handle errors.
     * Re-fetches the lock from pairedLocks after connect in case ownership migrated.
     */
    async _withLock(address, action) {
        const lock = this.pairedLocks.get(address);
        if (!lock) return false;
        if (!(await this._connectLock(lock))) return false;
        try {
            // Re-fetch in case _connectLock migrated ownership to a different proxy
            const currentLock = this.pairedLocks.get(address);
            return await action(currentLock);
        } catch (error) {
            console.error(error);
            return false;
        }
    }

    // ---------------------------------------------------------------------------
    // RSSI tracking
    // ---------------------------------------------------------------------------

    _updateRssi(proxyId, address, rssi) {
        if (!this.lockRssiMap.has(address)) {
            this.lockRssiMap.set(address, []);
        }
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
        setTimeout(() => this.proxies.forEach(p => p.client.startMonitor()), 200);
    }

    /**
     * Called when any proxy finds a lock during scan or monitor.
     * @param {import('ttlock-sdk-js').TTLock} lock
     * @param {string} proxyId  - which proxy found this lock
     */
    async _onFoundLock(lock, proxyId) {
        const address = lock.getAddress();

        // Always update RSSI — this is the core data for routing decisions
        this._updateRssi(proxyId, address, lock.rssi);

        let listChanged = false;

        if (lock.isPaired()) {
            if (!this.pairedLocks.has(address)) {
                // First sighting of this paired lock — record ownership
                this.lockOwnerMap.set(address, proxyId);
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
                // Already known — log that another proxy can also see it (useful for debugging)
                const ownerProxy = this.lockOwnerMap.get(address);
                if (proxyId !== ownerProxy) {
                    console.log(`Lock ${address} also seen by proxy [${proxyId}] rssi=${lock.rssi} (owner: [${ownerProxy}])`);
                }
            }

        } else if (!lock.isInitialized()) {
            if (!this.newLocks.has(address)) {
                this.lockOwnerMap.set(address, proxyId);
                console.log(`Discovered new lock: ${address} via [${proxyId}] rssi=${lock.rssi}`);
                this.newLocks.set(address, lock);
                listChanged = true;
                if (this.proxies.some(p => p.client.isScanning())) {
                    console.log("New lock found, stopping scan");
                    await this.stopScan();
                }
            }
        } else {
            try {
                console.log("Discovered unknown lock:", lock.toJSON());
            } catch (e) {
                console.log("Discovered unknown lock:", lock.getAddress(), "(toJSON failed - circular ref)");
            }
        }

        if (listChanged) {
            this.emit("lockListChanged");
        }
    }

    async _onUpdatedLockData() {
        const lockData = [];
        this.proxies.forEach(p => lockData.push(p.client.getLockData()));
        store.setLockData(lockData);
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
        if (lock.isPaired()) {
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