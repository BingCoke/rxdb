/**
 * Helper functions for the shared worker storage
 */
import { newRxError } from '../../rx-error';
import type {
    SharedWorkerMessage,
    SharedWorkerResponseMessage
} from './shared-worker-types';

/**
 * Creates a unique message id
 */
export function createMessageId(): string {
    return Math.random().toString(36).substring(2, 15) + 
           Math.random().toString(36).substring(2, 15);
}

/**
 * Sends a message to the worker and returns a promise that resolves with the response
 */
export function sendMessageToWorker(
    port: MessagePort,
    message: SharedWorkerMessage
): Promise<any> {
    return new Promise((resolve, reject) => {
        const messageId = message.id;
        
        // Create a handler for the response
        const responseHandler = (event: MessageEvent) => {
            const response = event.data as SharedWorkerResponseMessage;
            
            // Only handle responses for this message
            if (response.id !== messageId) {
                return;
            }
            
            // Remove the event listener
            port.removeEventListener('message', responseHandler);
            
            // Handle errors
            if (response.error) {
                const error = newRxError('RM1', {
                    error: {
                        name: response.error.name,
                        message: response.error.message
                    }
                });
                if (response.error.stack) {
                    error.stack = response.error.stack;
                }
                reject(error);
                return;
            }
            
            // Resolve with the result
            resolve(response.result);
        };
        
        // Add the event listener
        port.addEventListener('message', responseHandler);
        
        // Send the message
        port.postMessage(message);
    });
}

/**
 * Creates a SharedWorker instance from the input
 */
export function createSharedWorker(
    workerInput: string | (() => SharedWorker),
    workerOptions?: WorkerOptions
): SharedWorker {
    if (typeof workerInput === 'string') {
        return new SharedWorker(workerInput, workerOptions);
    } else {
        return workerInput();
    }
}

/**
 * Checks if the SharedWorker API is available
 */
export function hasSharedWorker(): boolean {
    return typeof SharedWorker !== 'undefined';
}
