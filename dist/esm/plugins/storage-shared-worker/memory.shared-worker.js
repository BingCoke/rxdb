/**
 * This is an example of a SharedWorker file that uses Memory storage as the base storage
 */
import { getRxStorageMemory } from '../../plugins/storage-memory';
import { exposeWorkerRxStorage } from './shared-worker';

// Create the base storage
var baseStorage = getRxStorageMemory();

// Expose the storage to the main thread
exposeWorkerRxStorage({
  storage: baseStorage
});

// Log that the worker is ready
console.log('Memory SharedWorker is ready');
//# sourceMappingURL=memory.shared-worker.js.map