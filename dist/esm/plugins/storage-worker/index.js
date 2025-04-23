/**
 * This file exports the Worker storage functionality
 */
import { getRxStorageWorker } from './rx-storage-worker';
import { exposeWorkerRxStorage } from './worker';
export { getRxStorageWorker, exposeWorkerRxStorage };

/**
 * Plugin that adds the Worker storage capabilities to RxDB
 */
export var RxDBWorkerStoragePlugin = {
  name: 'worker-storage',
  rxdb: true,
  overwritable: {
    createWorkerStorage: getRxStorageWorker
  }
};
//# sourceMappingURL=index.js.map