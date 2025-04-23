"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getRxStorageSharedWorker = getRxStorageSharedWorker;
var _rxjs = require("rxjs");
var _rxError = require("../../rx-error");
var _utils = require("../../plugins/utils");
var _sharedWorkerHelpers = require("./shared-worker-helpers");
/**
 * This file contains the RxStorage implementation for SharedWorker
 */

/**
 * Default settings for the shared worker storage
 */

/**
 * Default checkpoint type
 */

/**
 * Cache for the SharedWorker instances
 * so we do not create multiple instances for the same worker
 */
var SHARED_WORKER_CACHE = new Map();

/**
 * Creates a RxStorage that uses a SharedWorker
 */
function getRxStorageSharedWorker(settings) {
  // Check if SharedWorker is available
  if (!(0, _sharedWorkerHelpers.hasSharedWorker)()) {
    throw (0, _rxError.newRxError)('UT4', {
      adapter: 'SharedWorker is not available in this environment'
    });
  }
  var multiInstance = settings.multiInstance === false ? false : true;
  return {
    name: 'shared-worker',
    rxdbVersion: '14.0.0',
    async createStorageInstance(params) {
      var {
        databaseName,
        collectionName,
        schema,
        options = {},
        devMode,
        databaseInstanceToken,
        multiInstance: instanceMultiInstance
      } = params;

      // Get or create the SharedWorker instance
      var workerKey = typeof settings.workerInput === 'string' ? settings.workerInput : 'function-worker';
      var workerData = (0, _utils.getFromMapOrCreate)(SHARED_WORKER_CACHE, workerKey, () => {
        var worker = (0, _sharedWorkerHelpers.createSharedWorker)(settings.workerInput, settings.workerOptions);
        var port = worker.port;
        port.start();
        return {
          worker,
          port
        };
      });

      // Create the storage instance in the worker
      var instanceId = databaseName + '-' + collectionName;
      await (0, _sharedWorkerHelpers.sendMessageToWorker)(workerData.port, {
        type: 'create',
        id: (0, _sharedWorkerHelpers.createMessageId)(),
        databaseName,
        collectionName,
        schema,
        options,
        devMode,
        multiInstance: instanceMultiInstance
      });

      // Create the storage instance
      var storageInstance = {
        databaseName,
        collectionName,
        schema: schema,
        internals: {},
        options,
        bulkWrite(documentWrites, context) {
          return (0, _sharedWorkerHelpers.sendMessageToWorker)(workerData.port, {
            type: 'bulkPut',
            id: (0, _sharedWorkerHelpers.createMessageId)(),
            instanceId,
            documentWrites,
            context: context || ''
          });
        },
        findDocumentsById(documentIds, withDeleted) {
          return (0, _sharedWorkerHelpers.sendMessageToWorker)(workerData.port, {
            type: 'bulkGet',
            id: (0, _sharedWorkerHelpers.createMessageId)(),
            instanceId,
            documentIds,
            withDeleted
          });
        },
        query(preparedQuery) {
          return (0, _sharedWorkerHelpers.sendMessageToWorker)(workerData.port, {
            type: 'query',
            id: (0, _sharedWorkerHelpers.createMessageId)(),
            instanceId,
            preparedQuery
          });
        },
        count(preparedQuery) {
          return (0, _sharedWorkerHelpers.sendMessageToWorker)(workerData.port, {
            type: 'count',
            id: (0, _sharedWorkerHelpers.createMessageId)(),
            instanceId,
            preparedQuery
          });
        },
        getAttachmentData(documentId, attachmentId, digest) {
          return (0, _sharedWorkerHelpers.sendMessageToWorker)(workerData.port, {
            type: 'getAttachment',
            id: (0, _sharedWorkerHelpers.createMessageId)(),
            instanceId,
            documentId,
            attachmentId,
            digest
          });
        },
        cleanup(minimumDeletedTime) {
          return (0, _sharedWorkerHelpers.sendMessageToWorker)(workerData.port, {
            type: 'cleanup',
            id: (0, _sharedWorkerHelpers.createMessageId)(),
            instanceId,
            minimumDeletedTime
          });
        },
        async close() {
          await (0, _sharedWorkerHelpers.sendMessageToWorker)(workerData.port, {
            type: 'close',
            id: (0, _sharedWorkerHelpers.createMessageId)(),
            instanceId
          });
          return _utils.PROMISE_RESOLVE_VOID;
        },
        remove() {
          return (0, _sharedWorkerHelpers.sendMessageToWorker)(workerData.port, {
            type: 'remove',
            id: (0, _sharedWorkerHelpers.createMessageId)(),
            instanceId
          });
        },
        changeStream() {
          var subject = new _rxjs.Subject();
          var id = (0, _sharedWorkerHelpers.createMessageId)();
          var changeStreamHandler = event => {
            var data = event.data;
            if (data.type === 'changeStream' && data.id === id) {
              var changeEvent = data.changeEvent;
              subject.next(changeEvent);
            }
          };
          workerData.port.addEventListener('message', changeStreamHandler);

          // Start the change stream in the worker
          (0, _sharedWorkerHelpers.sendMessageToWorker)(workerData.port, {
            type: 'changeStream',
            id,
            instanceId
          });

          // When unsubscribed, clean up
          var originalSubscribe = subject.subscribe.bind(subject);
          subject.subscribe = function () {
            var subscription = originalSubscribe.apply(this, arguments);
            var originalUnsubscribe = subscription.unsubscribe.bind(subscription);
            subscription.unsubscribe = () => {
              workerData.port.removeEventListener('message', changeStreamHandler);
              // Stop the change stream in the worker
              (0, _sharedWorkerHelpers.sendMessageToWorker)(workerData.port, {
                type: 'unsubscribeChangeStream',
                id: (0, _sharedWorkerHelpers.createMessageId)(),
                instanceId,
                subscriptionId: id
              });
              originalUnsubscribe();
            };
            return subscription;
          };
          return subject;
        }
      };
      return storageInstance;
    }
  };
}
//# sourceMappingURL=rx-storage-shared-worker.js.map