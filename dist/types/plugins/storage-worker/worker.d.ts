/**
 * This file is the implementation of the Worker
 * It handles messages from the main thread and manages the storage
 */
import type { RxStorage } from '../../types';
/**
 * Expose the RxStorage to the outside
 * This function should be called in the worker file
 */
export declare function exposeWorkerRxStorage(options: {
    storage: RxStorage<any, any>;
}): void;
