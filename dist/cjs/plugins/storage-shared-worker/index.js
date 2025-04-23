"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RxDBSharedWorkerStoragePlugin = void 0;
Object.defineProperty(exports, "exposeWorkerRxStorage", {
  enumerable: true,
  get: function () {
    return _sharedWorker.exposeWorkerRxStorage;
  }
});
Object.defineProperty(exports, "getRxStorageSharedWorker", {
  enumerable: true,
  get: function () {
    return _rxStorageSharedWorker.getRxStorageSharedWorker;
  }
});
var _rxStorageSharedWorker = require("./rx-storage-shared-worker");
var _sharedWorker = require("./shared-worker");
/**
 * This file exports the SharedWorker storage functionality
 */

/**
 * Plugin that adds the SharedWorker storage capabilities to RxDB
 */
var RxDBSharedWorkerStoragePlugin = exports.RxDBSharedWorkerStoragePlugin = {
  name: 'shared-worker-storage',
  rxdb: true,
  overwritable: {
    createSharedWorkerStorage: _rxStorageSharedWorker.getRxStorageSharedWorker
  }
};
//# sourceMappingURL=index.js.map