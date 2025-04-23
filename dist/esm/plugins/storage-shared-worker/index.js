/**
 * This file exports the SharedWorker storage functionality
 */
import { getRxStorageSharedWorker } from './rx-storage-shared-worker';
import { exposeWorkerRxStorage } from './shared-worker';
export { getRxStorageSharedWorker, exposeWorkerRxStorage };

/**
 * Plugin that adds the SharedWorker storage capabilities to RxDB
 */
export var RxDBSharedWorkerStoragePlugin = {
  name: 'shared-worker-storage',
  rxdb: true,
  overwritable: {
    createSharedWorkerStorage: getRxStorageSharedWorker
  }
};
//# sourceMappingURL=index.js.map