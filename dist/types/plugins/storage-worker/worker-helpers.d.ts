import type { WorkerMessage } from './worker-types';
/**
 * Creates a unique message id
 */
export declare function createMessageId(): string;
/**
 * Sends a message to the worker and returns a promise that resolves with the response
 */
export declare function sendMessageToWorker(worker: Worker, message: WorkerMessage): Promise<any>;
/**
 * Creates a Worker instance from the input
 */
export declare function createWorker(workerInput: string | (() => Worker), workerOptions?: WorkerOptions): Worker;
/**
 * Checks if the Worker API is available
 */
export declare function hasWorker(): boolean;
