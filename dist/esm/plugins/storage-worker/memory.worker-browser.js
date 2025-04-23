/**
 * This is a standalone Worker file that uses Memory storage as the base storage
 * It can be directly used in the browser
 */

// Import the memory storage
import { getRxStorageMemory } from '../../plugins/storage-memory';

// Import the worker helper
import { exposeWorkerRxStorage } from './worker';
import { RxDBUpdatePlugin } from '../update';
import { addRxPlugin } from '../../plugin';
addRxPlugin(RxDBUpdatePlugin);

// Create the base storage
var baseStorage = getRxStorageMemory();

// Expose the storage to the main thread
exposeWorkerRxStorage({
  storage: baseStorage
});

// Log that the worker is ready
console.log('Memory Worker is ready');
//# sourceMappingURL=memory.worker-browser.js.map