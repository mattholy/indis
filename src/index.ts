interface StoredValue<T = any> {
    value: T
    expiresAt?: number
}

export class Indis {
    private dbName: string
    private storeName: string
    private db?: IDBDatabase
    private channels: Map<string, Set<(message: any) => void>> = new Map();
    private broadcaster?: BroadcastChannel

    constructor(dbName = 'indis-storage', storeName = 'kv') {
        this.dbName = dbName
        this.storeName = storeName
        if (typeof BroadcastChannel !== 'undefined') {
            this.broadcaster = new BroadcastChannel(`indis-${dbName}`)
            this.broadcaster.onmessage = (event) => {
                const { channel, message } = event.data
                const handlers = this.channels.get(channel)
                if (handlers) {
                    for (const fn of handlers) {
                        fn(message)
                    }
                }
            }
        }
    }

    private getDB(): Promise<IDBDatabase> {
        if (this.db) return Promise.resolve(this.db)

        return new Promise((resolve, reject) => {
            const request = indexedDB.open(this.dbName, 1)
            request.onupgradeneeded = () => {
                const db = request.result
                if (!db.objectStoreNames.contains(this.storeName)) {
                    db.createObjectStore(this.storeName)
                }
            }
            request.onsuccess = () => {
                this.db = request.result
                resolve(this.db)
            }
            request.onerror = () => reject(request.error)
        })
    }

    private async readRaw(key: string): Promise<StoredValue | undefined> {
        const db = await this.getDB()
        return new Promise((resolve, reject) => {
            const tx = db.transaction(this.storeName, 'readonly')
            const store = tx.objectStore(this.storeName)
            const req = store.get(key)
            req.onsuccess = () => resolve(req.result)
            req.onerror = () => reject(req.error)
        })
    }

    async set(key: string, value: any, ttl?: number): Promise<void> {
        const db = await this.getDB()
        const data: StoredValue = {
            value,
            expiresAt: ttl ? Date.now() + ttl : undefined,
        }
        return new Promise((resolve, reject) => {
            const tx = db.transaction(this.storeName, 'readwrite')
            const store = tx.objectStore(this.storeName)
            const req = store.put(data, key)
            req.onsuccess = () => resolve()
            req.onerror = () => reject(req.error)
        })
    }

    async get(key: string): Promise<any> {
        const data = await this.readRaw(key)
        if (!data) return undefined
        if (data.expiresAt && data.expiresAt < Date.now()) {
            await this.del(key)
            return undefined
        }
        return data.value
    }

    async del(key: string): Promise<void> {
        const db = await this.getDB()
        return new Promise((resolve, reject) => {
            const tx = db.transaction(this.storeName, 'readwrite')
            const store = tx.objectStore(this.storeName)
            const req = store.delete(key)
            req.onsuccess = () => resolve()
            req.onerror = () => reject(req.error)
        })
    }

    async expire(key: string, ttl: number): Promise<boolean> {
        const data = await this.readRaw(key)
        if (!data) return false
        data.expiresAt = Date.now() + ttl
        await this.set(key, data.value, ttl)
        return true
    }

    async ttl(key: string): Promise<number> {
        const data = await this.readRaw(key)
        if (!data) return -2
        if (!data.expiresAt) return -1
        const remaining = data.expiresAt - Date.now()
        return remaining > 0 ? remaining : -2
    }

    async keys(): Promise<string[]> {
        const db = await this.getDB()
        return new Promise((resolve, reject) => {
            const tx = db.transaction(this.storeName, 'readonly')
            const store = tx.objectStore(this.storeName)
            const req = store.getAllKeys()
            req.onsuccess = () => resolve(req.result as string[])
            req.onerror = () => reject(req.error)
        })
    }

    async clear(): Promise<void> {
        const db = await this.getDB()
        return new Promise((resolve, reject) => {
            const tx = db.transaction(this.storeName, 'readwrite')
            const store = tx.objectStore(this.storeName)
            const req = store.clear()
            req.onsuccess = () => resolve()
            req.onerror = () => reject(req.error)
        })
    }

    async incr(key: string): Promise<number> {
        const current = parseInt(await this.get(key)) || 0
        const next = current + 1
        await this.set(key, next)
        return next
    }

    async decr(key: string): Promise<number> {
        const current = parseInt(await this.get(key)) || 0
        const next = current - 1
        await this.set(key, next)
        return next
    }

    async hset(key: string, field: string, value: any): Promise<void> {
        const obj = (await this.get(key)) || {}
        obj[field] = value
        await this.set(key, obj)
    }

    async hget(key: string, field: string): Promise<any> {
        const obj = await this.get(key)
        return obj ? obj[field] : undefined
    }

    async hdel(key: string, field: string): Promise<void> {
        const obj = await this.get(key)
        if (obj && field in obj) {
            delete obj[field]
            await this.set(key, obj)
        }
    }

    async hgetall(key: string): Promise<Record<string, any>> {
        return (await this.get(key)) || {}
    }

    async lpush(key: string, value: any): Promise<number> {
        const arr: any[] = (await this.get(key)) || []
        arr.unshift(value)
        await this.set(key, arr)
        return arr.length
    }

    async rpush(key: string, value: any): Promise<number> {
        const arr: any[] = (await this.get(key)) || []
        arr.push(value)
        await this.set(key, arr)
        return arr.length
    }

    async lpop(key: string): Promise<any> {
        const arr: any[] = (await this.get(key)) || []
        const val = arr.shift()
        await this.set(key, arr)
        return val
    }

    async rpop(key: string): Promise<any> {
        const arr: any[] = (await this.get(key)) || []
        const val = arr.pop()
        await this.set(key, arr)
        return val
    }

    async llen(key: string): Promise<number> {
        const arr: any[] = (await this.get(key)) || []
        return arr.length
    }

    publish(channel: string, message: any): void {
        if (this.broadcaster) {
            this.broadcaster.postMessage({ channel, message })
        }
    }

    subscribe(channel: string, handler: (message: any) => void): void {
        if (!this.channels.has(channel)) {
            this.channels.set(channel, new Set())
        }
        this.channels.get(channel)!.add(handler)
    }

    unsubscribe(channel: string, handler: (message: any) => void): void {
        const handlers = this.channels.get(channel)
        if (handlers) {
            handlers.delete(handler)
            if (handlers.size === 0) {
                this.channels.delete(channel)
            }
        }
    }
}
