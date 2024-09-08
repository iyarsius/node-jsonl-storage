import { createWriteStream, createReadStream, unlinkSync, renameSync, existsSync, appendFileSync } from "fs";
import readline from "readline";

export interface INodeJsonlStorageOptions {
    name: string;
    folder?: string;
};

export type NodeJsonlStorageIteratorCallback<T, U> = (item: T, key: U) => void;

export class NodeJsonlStorage<key = string, item = any> {
    protected path: string;
    protected isWriteStreamBusy = false;
    protected isRemoveItemBusy: boolean = false;
    protected isSetItemBusy: boolean = false;

    protected setItemQueue: { key: key, item: item }[] = [];
    protected removeItemQueue: key[] = [];

    constructor(protected options: INodeJsonlStorageOptions) {
        this.path = this.options.folder ? `${this.options.folder}/${this.options.name}.jsonl`.replace('//', '/') : `${this.options.name}.jsonl`;
        // create the file
        if (!existsSync(this.path)) appendFileSync(this.path, '');
    };

    protected async waitForWrite() {
        while (this.isWriteStreamBusy) await new Promise((x) => setTimeout(x, 5));
    };

    clear() {
        this.isWriteStreamBusy = true;
        unlinkSync(this.path);
        appendFileSync(this.path, '');
        this.isWriteStreamBusy = false;
    }

    async getItem(key: key): Promise<item | null> {
        await this.waitForWrite();

        const stream = createReadStream(this.path);
        const rl = readline.createInterface({
            input: stream,
            crlfDelay: Infinity
        });

        return await new Promise(resolve => {
            let found: item;
            rl.on('line', (line) => {
                const json = JSON.parse(line);
                if (json.key === key) {
                    found = json.data;
                    rl.close();
                }
            });

            rl.on("close", () => resolve(found))
        });
    }

    async iterate(iteratorCallback: NodeJsonlStorageIteratorCallback<item, key>): Promise<void> {
        await this.waitForWrite();

        const stream = createReadStream(this.path);
        const rl = readline.createInterface({
            input: stream,
            crlfDelay: Infinity
        });

        await new Promise(resolve => {
            rl.on('line', (line) => {
                const json = JSON.parse(line);
                iteratorCallback(json.data, json.key);
            });

            rl.on("close", () => resolve(null))
        });
    }

    async key(keyIndex: number): Promise<key | null> {
        await this.waitForWrite();

        const stream = createReadStream(this.path);
        const rl = readline.createInterface({
            input: stream,
            crlfDelay: Infinity
        });

        return await new Promise(resolve => {
            let index = 0;
            let found: key;
            rl.on('line', (line) => {
                index++
                if (keyIndex === index) {
                    found = JSON.parse(line).key;
                    rl.close();
                }
            });

            rl.on("close", () => resolve(found))
        });
    }

    async keys(): Promise<key[]> {
        await this.waitForWrite();

        const stream = createReadStream(this.path);
        const rl = readline.createInterface({
            input: stream,
            crlfDelay: Infinity
        });

        return await new Promise(resolve => {
            let keys: key[] = [];
            rl.on('line', (line) => {
                keys.push(JSON.parse(line).key);
            });

            rl.on("close", () => resolve(keys))
        });
    }

    async length(): Promise<number> {
        await this.waitForWrite();

        const stream = createReadStream(this.path);
        const rl = readline.createInterface({
            input: stream,
            crlfDelay: Infinity
        });

        return await new Promise(resolve => {
            let index = 0;
            rl.on('line', () => index++);
            rl.on("close", () => resolve(index))
        });
    }

    async removeItem(key: key): Promise<void> {
        this.removeItemQueue.push(key);

        if (this.isRemoveItemBusy) return;
        this.isRemoveItemBusy = true;

        await this.waitForWrite();
        this.isWriteStreamBusy = true;

        const stream = createReadStream(this.path);
        const rl = readline.createInterface({
            input: stream,
            crlfDelay: Infinity
        });

        const tempPath = `${this.path}`.replace('.jsonl', "-temp.jsonl");
        const tempFileStream = createWriteStream(tempPath);

        await new Promise(resolve => {
            const queue = this.removeItemQueue.splice(0, this.removeItemQueue.length + 1);

            rl.on('line', (line) => {
                const json = JSON.parse(line);
                if (queue.includes(json.key)) return;

                tempFileStream.write(`${line}\n`)
            });
            rl.on("close", () => {
                tempFileStream.end();
                resolve(null);
            });
        });

        unlinkSync(this.path);
        renameSync(tempPath, this.path);

        this.isWriteStreamBusy = false;
        this.isRemoveItemBusy = false;
    };

    protected async _insertSetItemQueue(): Promise<void> {
        const stream = createReadStream(this.path);
        const rl = readline.createInterface({
            input: stream,
            crlfDelay: Infinity
        });

        const tempPath = `${this.path}`.replace('.jsonl', "-temp.jsonl");
        const tempFileStream = createWriteStream(tempPath);

        await new Promise(resolve => {
            const queue = this.setItemQueue.splice(0, this.setItemQueue.length + 1);
            const queueMap = new Map(queue.map(({ key, item }) => [key, item]));

            rl.on('line', (line) => {
                const json = JSON.parse(line);
                const queueItem = queueMap.get(json.key);
                if (queueItem) {
                    line = JSON.stringify({ key: json.key, data: queueItem });
                    queueMap.delete(json.key);
                }

                tempFileStream.write(`${line}\n`)
            });
            rl.on("close", () => {
                if (queueMap.size > 0) {
                    const arr = Array.from(queueMap, ([key, value]) => ({ key: key, data: value }));
                    const str = arr.map(obj => JSON.stringify(obj)).join('\n');
                    tempFileStream.write(str);
                }
                tempFileStream.end();
                resolve(null);
            });
        });

        unlinkSync(this.path);
        renameSync(tempPath, this.path);
    }

    async setItem(key: key, item: item): Promise<item> {
        this.setItemQueue.push({ key, item });

        if (this.isSetItemBusy) return item;
        this.isSetItemBusy = true;

        await this.waitForWrite();
        this.isWriteStreamBusy = true;

        await this._insertSetItemQueue();

        this.isWriteStreamBusy = false;
        this.isSetItemBusy = false;

        return item;
    };
};