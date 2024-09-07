# A simple interface for jsonl storage.

```bash
npm i node-jsonl-storage
```

This library is really simple. it handle local storage with jsonl (json line) files.
The goal is to allow to use JSONL files as a simple key/value storage and without overhead memory.
The module use streams to limit memory usage.

Here is some examples of usage:

```ts
import { NodeJsonlStorage } from 'node-jsonl-storage';
import { MyModel } from './model.ts';

// by default the jsonl files will be stored in the root folder
const storage = new NodeJsonlStorage<string, MyModel>({
    name: "myData"
});

// but you can specify a custom path to store them
const storage = new NodeJsonlStorage<string, MyModel>({
    name:'myData',
    folder: './data'
});
```

The methods mimic the key/value storages. By defining types in the storage declaration
all methods will be type safied.

```ts
// create or update an item
const item = await storage.setItem("key", item);

// retrieve an item
const item = await storage.getItem("key");

// delete an item
await storage.deleteItem("key");

/**iterate over all items
*
* there is no .all() method because it would be costly to load everything into memory
* instead we provide an iterator method that will return a stream of items
* this is more efficient than using a method that would load all items
*/
await storage.iterate((item) => console.log(item))

// get the number of items in the storage
const length = await storage.length();

// remove all items
await storage.clear();

// get a key based on the index
const key = await storage.key(index);

// get all keys in the storage
const keys = await storage.keys();
```

This interface will be plugged into others modules, so others similar modules will be implemented
allowing compression, cryptography or others environments compatibility.