var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
export class Indis {
    constructor(dbName = 'indis-storage', storeName = 'kv') {
        this.channels = new Map();
        this.dbName = dbName;
        this.storeName = storeName;
        if (typeof BroadcastChannel !== 'undefined') {
            this.broadcaster = new BroadcastChannel(`indis-${dbName}`);
            this.broadcaster.onmessage = (event) => {
                const { channel, message } = event.data;
                const handlers = this.channels.get(channel);
                if (handlers) {
                    for (const fn of handlers) {
                        fn(message);
                    }
                }
            };
        }
    }
    getDB() {
        if (this.db)
            return Promise.resolve(this.db);
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(this.dbName, 1);
            request.onupgradeneeded = () => {
                const db = request.result;
                if (!db.objectStoreNames.contains(this.storeName)) {
                    db.createObjectStore(this.storeName);
                }
            };
            request.onsuccess = () => {
                this.db = request.result;
                resolve(this.db);
            };
            request.onerror = () => reject(request.error);
        });
    }
    readRaw(key) {
        return __awaiter(this, void 0, void 0, function* () {
            const db = yield this.getDB();
            return new Promise((resolve, reject) => {
                const tx = db.transaction(this.storeName, 'readonly');
                const store = tx.objectStore(this.storeName);
                const req = store.get(key);
                req.onsuccess = () => resolve(req.result);
                req.onerror = () => reject(req.error);
            });
        });
    }
    set(key, value, ttl) {
        return __awaiter(this, void 0, void 0, function* () {
            const db = yield this.getDB();
            const data = {
                value,
                expiresAt: ttl ? Date.now() + ttl : undefined,
            };
            return new Promise((resolve, reject) => {
                const tx = db.transaction(this.storeName, 'readwrite');
                const store = tx.objectStore(this.storeName);
                const req = store.put(data, key);
                req.onsuccess = () => resolve();
                req.onerror = () => reject(req.error);
            });
        });
    }
    get(key) {
        return __awaiter(this, void 0, void 0, function* () {
            const data = yield this.readRaw(key);
            if (!data)
                return undefined;
            if (data.expiresAt && data.expiresAt < Date.now()) {
                yield this.del(key);
                return undefined;
            }
            return data.value;
        });
    }
    del(key) {
        return __awaiter(this, void 0, void 0, function* () {
            const db = yield this.getDB();
            return new Promise((resolve, reject) => {
                const tx = db.transaction(this.storeName, 'readwrite');
                const store = tx.objectStore(this.storeName);
                const req = store.delete(key);
                req.onsuccess = () => resolve();
                req.onerror = () => reject(req.error);
            });
        });
    }
    expire(key, ttl) {
        return __awaiter(this, void 0, void 0, function* () {
            const data = yield this.readRaw(key);
            if (!data)
                return false;
            data.expiresAt = Date.now() + ttl;
            yield this.set(key, data.value, ttl);
            return true;
        });
    }
    ttl(key) {
        return __awaiter(this, void 0, void 0, function* () {
            const data = yield this.readRaw(key);
            if (!data)
                return -2;
            if (!data.expiresAt)
                return -1;
            const remaining = data.expiresAt - Date.now();
            return remaining > 0 ? remaining : -2;
        });
    }
    keys() {
        return __awaiter(this, void 0, void 0, function* () {
            const db = yield this.getDB();
            return new Promise((resolve, reject) => {
                const tx = db.transaction(this.storeName, 'readonly');
                const store = tx.objectStore(this.storeName);
                const req = store.getAllKeys();
                req.onsuccess = () => resolve(req.result);
                req.onerror = () => reject(req.error);
            });
        });
    }
    clear() {
        return __awaiter(this, void 0, void 0, function* () {
            const db = yield this.getDB();
            return new Promise((resolve, reject) => {
                const tx = db.transaction(this.storeName, 'readwrite');
                const store = tx.objectStore(this.storeName);
                const req = store.clear();
                req.onsuccess = () => resolve();
                req.onerror = () => reject(req.error);
            });
        });
    }
    incr(key) {
        return __awaiter(this, void 0, void 0, function* () {
            const current = parseInt(yield this.get(key)) || 0;
            const next = current + 1;
            yield this.set(key, next);
            return next;
        });
    }
    decr(key) {
        return __awaiter(this, void 0, void 0, function* () {
            const current = parseInt(yield this.get(key)) || 0;
            const next = current - 1;
            yield this.set(key, next);
            return next;
        });
    }
    hset(key, field, value) {
        return __awaiter(this, void 0, void 0, function* () {
            const obj = (yield this.get(key)) || {};
            obj[field] = value;
            yield this.set(key, obj);
        });
    }
    hget(key, field) {
        return __awaiter(this, void 0, void 0, function* () {
            const obj = yield this.get(key);
            return obj ? obj[field] : undefined;
        });
    }
    hdel(key, field) {
        return __awaiter(this, void 0, void 0, function* () {
            const obj = yield this.get(key);
            if (obj && field in obj) {
                delete obj[field];
                yield this.set(key, obj);
            }
        });
    }
    hgetall(key) {
        return __awaiter(this, void 0, void 0, function* () {
            return (yield this.get(key)) || {};
        });
    }
    lpush(key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            const arr = (yield this.get(key)) || [];
            arr.unshift(value);
            yield this.set(key, arr);
            return arr.length;
        });
    }
    rpush(key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            const arr = (yield this.get(key)) || [];
            arr.push(value);
            yield this.set(key, arr);
            return arr.length;
        });
    }
    lpop(key) {
        return __awaiter(this, void 0, void 0, function* () {
            const arr = (yield this.get(key)) || [];
            const val = arr.shift();
            yield this.set(key, arr);
            return val;
        });
    }
    rpop(key) {
        return __awaiter(this, void 0, void 0, function* () {
            const arr = (yield this.get(key)) || [];
            const val = arr.pop();
            yield this.set(key, arr);
            return val;
        });
    }
    llen(key) {
        return __awaiter(this, void 0, void 0, function* () {
            const arr = (yield this.get(key)) || [];
            return arr.length;
        });
    }
    publish(channel, message) {
        if (this.broadcaster) {
            this.broadcaster.postMessage({ channel, message });
        }
    }
    subscribe(channel, handler) {
        if (!this.channels.has(channel)) {
            this.channels.set(channel, new Set());
        }
        this.channels.get(channel).add(handler);
    }
    unsubscribe(channel, handler) {
        const handlers = this.channels.get(channel);
        if (handlers) {
            handlers.delete(handler);
            if (handlers.size === 0) {
                this.channels.delete(channel);
            }
        }
    }
}
