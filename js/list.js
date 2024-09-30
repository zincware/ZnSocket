// Python list uses
// llen, lindex, lset, lrem, rpush, lpush, linsert, lrange, rpush

export class List {
  constructor({ client, key, callbacks }) {
    this._client = client;
    this._key = key;
    this._callbacks = callbacks;
  }

  async len() {
    return this._client.lLen(this._key);
  }

  async append(value) {
    if (this._callbacks && this._callbacks.append) {
      await this._callbacks.append(value);
    }
    return this._client.rPush(this._key, JSON.stringify(value));
  }

  async setitem(index, value) {
    if (this._callbacks && this._callbacks.setitem) {
      await this._callbacks.setitem(value);
    }
    return this._client.lSet(this._key, index, JSON.stringify(value));
  }

  async getitem(index) {
    const value = await this._client.lIndex(this._key, index);
    if (value === null) {
      return null;
    }
    return JSON.parse(value);
  }

  [Symbol.asyncIterator]() {
    let index = 0;
    let length;
  
    return {
      next: async () => {
        // Get the current length of the list from the List instance
        if (length === undefined){
          // only get it once, for better performance / might miss some updates
          length = await this.len();
        }
  
        // Check if we've reached the end of the list
        if (index >= length) {
          return { value: undefined, done: true };
        }
  
        // Get the item at the current index from the List instance
        const value = await this.getitem(index);
        index += 1;

        return { value, done: false };
      }
    };
  }
}
