"use strict";

var _storageMemory = require("../../plugins/storage-memory");
var _sharedWorker = require("./shared-worker");
/**
 * This is a standalone SharedWorker file that uses Memory storage as the base storage
 * It can be directly used in the browser
 */

// Import the memory storage

// Import the worker helper

// Create the base storage
var baseStorage = (0, _storageMemory.getRxStorageMemory)();

// Expose the storage to the main thread
(0, _sharedWorker.exposeWorkerRxStorage)({
  storage: baseStorage
});

// Log that the worker is ready
console.log('Memory SharedWorker is ready');
//# sourceMappingURL=memory.shared-worker-browser.js.map