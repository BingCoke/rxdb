"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RxDBWorkerStoragePlugin = void 0;
Object.defineProperty(exports, "exposeWorkerRxStorage", {
  enumerable: true,
  get: function () {
    return _worker.exposeWorkerRxStorage;
  }
});
Object.defineProperty(exports, "getRxStorageWorker", {
  enumerable: true,
  get: function () {
    return _rxStorageWorker.getRxStorageWorker;
  }
});
var _rxStorageWorker = require("./rx-storage-worker");
var _worker = require("./worker");
/**
 * This file exports the Worker storage functionality
 */

/**
 * Plugin that adds the Worker storage capabilities to RxDB
 */
var RxDBWorkerStoragePlugin = exports.RxDBWorkerStoragePlugin = {
  name: 'worker-storage',
  rxdb: true,
  overwritable: {
    createWorkerStorage: _rxStorageWorker.getRxStorageWorker
  }
};
//# sourceMappingURL=index.js.map