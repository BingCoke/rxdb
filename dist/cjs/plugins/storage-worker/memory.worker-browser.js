"use strict";

var _storageMemory = require("../../plugins/storage-memory");
var _worker = require("./worker");
var _update = require("../update");
var _plugin = require("../../plugin");
/**
 * This is a standalone Worker file that uses Memory storage as the base storage
 * It can be directly used in the browser
 */

// Import the memory storage

// Import the worker helper

(0, _plugin.addRxPlugin)(_update.RxDBUpdatePlugin);

// Create the base storage
var baseStorage = (0, _storageMemory.getRxStorageMemory)();

// Expose the storage to the main thread
(0, _worker.exposeWorkerRxStorage)({
  storage: baseStorage
});

// Log that the worker is ready
console.log('Memory Worker is ready');
//# sourceMappingURL=memory.worker-browser.js.map