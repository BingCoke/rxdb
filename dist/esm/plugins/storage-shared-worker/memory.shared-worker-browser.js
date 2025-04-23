/**
 * This is a standalone SharedWorker file that uses Memory storage as the base storage
 * It can be directly used in the browser
 */

// Import the memory storage
import { getRxStorageMemory } from '../../plugins/storage-memory';

// Import the worker helper
import { exposeWorkerRxStorage } from './shared-worker';

// Create the base storage
var baseStorage = getRxStorageMemory();

// Expose the storage to the main thread
exposeWorkerRxStorage({
  storage: baseStorage
});

// Log that the worker is ready
console.log('Memory SharedWorker is ready');
//# sourceMappingURL=memory.shared-worker-browser.js.map