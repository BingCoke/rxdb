/**
 * This file exports the Worker storage functionality
 */
import { getRxStorageWorker } from './rx-storage-worker';
import type { RxStorageWorkerSettings } from './worker-types';
import { exposeWorkerRxStorage } from './worker';

export {
    getRxStorageWorker,
    RxStorageWorkerSettings,
    exposeWorkerRxStorage
};

/**
 * Plugin that adds the Worker storage capabilities to RxDB
 */
export const RxDBWorkerStoragePlugin = {
    name: 'worker-storage',
    rxdb: true,
    overwritable: {
        createWorkerStorage: getRxStorageWorker
    }
};
