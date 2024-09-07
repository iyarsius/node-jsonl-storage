import { createWriteStream, createReadStream, unlinkSync, renameSync, existsSync, appendFileSync } from "fs";
import readline from "readline";

export interface INodeJsonlStorageOptions {
    name: string;
    folder?: string;
};

export type NodeJsonlStorageIteratorCallback<T, U> = (item?: T, key?: U) => void;

export class NodeJsonlStorage<key = string, item = any> {
    protected path: string;

    constructor(protected options: INodeJsonlStorageOptions) {
        this.path = this.options.folder ? `${this.options.folder}/${this.options.name}.jsonl`.replace('//', '/') : `${this.options.name}.jsonl`;
        // create the file
        if (!existsSync(this.path)) appendFileSync(this.path, '');
    }

    clear() {
        unlinkSync(this.path);
        appendFileSync(this.path, '');
    }

    async getItem(key: key): Promise<item | null> {
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
        const stream = createReadStream(this.path);
        const rl = readline.createInterface({
            input: stream,
            crlfDelay: Infinity
        });

        const tempPath = `${this.path}`.replace('.jsonl', "-temp.jsonl");
        const tempFileStream = createWriteStream(tempPath);

        await new Promise(resolve => {
            rl.on('line', (line) => {
                const json = JSON.parse(line);
                if (json.key === key) return;

                tempFileStream.write(`${line}\n`)
            });
            rl.on("close", () => {
                tempFileStream.end();
                resolve(null);
            });
        });

        unlinkSync(this.path);
        renameSync(tempPath, this.path)
    }

    async setItem(key: key, item: item): Promise<item> {
        const stream = createReadStream(this.path);
        const rl = readline.createInterface({
            input: stream,
            crlfDelay: Infinity
        });

        const tempPath = `${this.path}`.replace('.jsonl', "-temp.jsonl");
        const tempFileStream = createWriteStream(tempPath);

        await new Promise(resolve => {
            const stringified = JSON.stringify({ key: key, data: item });
            let updated = false;

            rl.on('line', (line) => {
                const json = JSON.parse(line);
                if (json.key === key) {
                    line = stringified;
                    updated = true;
                }

                tempFileStream.write(`${line}\n`)
            });
            rl.on("close", () => {
                if (!updated) tempFileStream.write(`${stringified}\n`);
                tempFileStream.end();
                resolve(null);
            });
        });

        unlinkSync(this.path);
        renameSync(tempPath, this.path);

        return item;
    };
};