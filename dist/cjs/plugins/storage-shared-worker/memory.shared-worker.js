"use strict";

var _storageMemory = require("../../plugins/storage-memory");
var _sharedWorker = require("./shared-worker");
/**
 * This is an example of a SharedWorker file that uses Memory storage as the base storage
 */

// Create the base storage
var baseStorage = (0, _storageMemory.getRxStorageMemory)();

// Expose the storage to the main thread
(0, _sharedWorker.exposeWorkerRxStorage)({
  storage: baseStorage
});

// Log that the worker is ready
console.log('Memory SharedWorker is ready');
//# sourceMappingURL=memory.shared-worker.js.map