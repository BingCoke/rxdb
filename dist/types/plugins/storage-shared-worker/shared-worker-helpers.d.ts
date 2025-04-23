import type { SharedWorkerMessage } from './shared-worker-types';
/**
 * Creates a unique message id
 */
export declare function createMessageId(): string;
/**
 * Sends a message to the worker and returns a promise that resolves with the response
 */
export declare function sendMessageToWorker(port: MessagePort, message: SharedWorkerMessage): Promise<any>;
/**
 * Creates a SharedWorker instance from the input
 */
export declare function createSharedWorker(workerInput: string | (() => SharedWorker), workerOptions?: WorkerOptions): SharedWorker;
/**
 * Checks if the SharedWorker API is available
 */
export declare function hasSharedWorker(): boolean;
