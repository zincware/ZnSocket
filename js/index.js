import { io } from "socket.io-client";  // Removed the unnecessary 'constants' import

export class Client {
    constructor(url, namespace = "znsocket") {
        // Correct concatenation of URL and namespace for socket connection
        // const path =`${url}/${namespace}`
        // throw new Error(url); // logs ws://127.0.0.1:10063/znsocket
        this._socket = io(url);
    }

    lLen(key) {
        return new Promise((resolve, reject) => {
            this._socket.emit("llen", key, (data) => {
                // Check if there is an error or invalid response and reject if necessary
                if (data && data.error) {
                    reject(data.error);
                } else {
                    resolve(data);
                }
            });
        });
    }
}

// Python list uses
// llen, lindex, lset, lrem, rpush, lpush, linsert, lrange, rpush

// Python dict uses
// hget, hset, hdel, hexists, hlen, hkeys, hvals, hgetall

export class List {
    constructor(client, key) {
        this._client = client
        this._key = key
    }

    async len() {
        return this._client.lLen(this._key)
    }

    async append(value){
        return this._client.rPush(this._key, JSON.stringify(value))

    }

    async setitem(index, value){
       return this._client.lSet(this._key, index, JSON.stringify(value))
    }

    async getitem(index){
        const value = await this._client.lIndex(this._key, index)
        if (value === null) {
            return null
        }
        return JSON.parse(value)
    }

    [Symbol.asyncIterator]() {
        let index = 0;
        return {
            next: async () => {
                const value = await this.getitem(index);
                index += 1;
                return {value, done: value === null};
            }
        }
    }
}

export class Dict{
    constructor(client, key) {
        this._client = client
        this._key = key
    }

    async len() {
        return this._client.hLen(this._key)
    }

    async setitem(key, value){
        return this._client.hSet(this._key, JSON.stringify(key), JSON.stringify(value))
    }

    async getitem(key){
        const value = await this._client.hGet(this._key, JSON.stringify(key))
        if (value === null) {
            return null
        }
        return JSON.stringify(value)
    }

    async keys(){
        return this._client.hKeys(this._key)
    }

    async values(){
        // JSON
        return this._client.hVals(this._key)
    }

    async items(){
        // JSON
        return this._client.hGetAll(this._key)
    }
}
