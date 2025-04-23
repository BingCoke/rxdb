"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.createMessageId = createMessageId;
exports.createSharedWorker = createSharedWorker;
exports.hasSharedWorker = hasSharedWorker;
exports.sendMessageToWorker = sendMessageToWorker;
var _rxError = require("../../rx-error");
/**
 * Helper functions for the shared worker storage
 */

/**
 * Creates a unique message id
 */
function createMessageId() {
  return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
}

/**
 * Sends a message to the worker and returns a promise that resolves with the response
 */
function sendMessageToWorker(port, message) {
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
      port.removeEventListener('message', responseHandler);

      // Handle errors
      if (response.error) {
        var error = (0, _rxError.newRxError)('RM1', {
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
function createSharedWorker(workerInput, workerOptions) {
  if (typeof workerInput === 'string') {
    return new SharedWorker(workerInput, workerOptions);
  } else {
    return workerInput();
  }
}

/**
 * Checks if the SharedWorker API is available
 */
function hasSharedWorker() {
  return typeof SharedWorker !== 'undefined';
}
//# sourceMappingURL=shared-worker-helpers.js.map