/**
 * This file is the implementation of the SharedWorker
 * It handles messages from the main thread and manages the storage
 */
import type { RxStorage } from '../../types';
/**
 * Expose the RxStorage to the outside
 * This function should be called in the shared worker file
 */
export declare function exposeWorkerRxStorage(options: {
    storage: RxStorage<any, any>;
}): void;
