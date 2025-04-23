/**
 * Helper functions for the worker storage
 */
import { newRxError } from '../../rx-error';
/**
 * Creates a unique message id
 */
export function createMessageId() {
  return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
}

/**
 * Sends a message to the worker and returns a promise that resolves with the response
 */
export function sendMessageToWorker(worker, message) {
  return new Promise((resolve, reject) => {
    var messageId = message.id;

    // Create a handler for the response
    var responseHandler = event => {
      var response = event.data;

      // Only handle responses for this message
      if (response.id !== messageId) {
        return;
      }

      // Remove the event listener
      worker.removeEventListener('message', responseHandler);

      // Handle errors
      if (response.error) {
        var error = newRxError('RM1', {
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
    worker.addEventListener('message', responseHandler);

    // Send the message
    worker.postMessage(message);
  });
}

/**
 * Creates a Worker instance from the input
 */
export function createWorker(workerInput, workerOptions) {
  if (typeof workerInput === 'string') {
    return new Worker(workerInput, workerOptions);
  } else {
    return workerInput();
  }
}

/**
 * Checks if the Worker API is available
 */
export function hasWorker() {
  return typeof Worker !== 'undefined';
}
//# sourceMappingURL=worker-helpers.js.map