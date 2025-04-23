/**
 * This file exports the SharedWorker storage functionality
 */
import { getRxStorageSharedWorker } from './rx-storage-shared-worker';
import type { RxStorageSharedWorkerSettings } from './shared-worker-types';
import { exposeWorkerRxStorage } from './shared-worker';

export {
    getRxStorageSharedWorker,
    RxStorageSharedWorkerSettings,
    exposeWorkerRxStorage
};

/**
 * Plugin that adds the SharedWorker storage capabilities to RxDB
 */
export const RxDBSharedWorkerStoragePlugin = {
    name: 'shared-worker-storage',
    rxdb: true,
    overwritable: {
        createSharedWorkerStorage: getRxStorageSharedWorker
    }
};
