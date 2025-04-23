"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getRxStorageWorker = getRxStorageWorker;
var _rxjs = require("rxjs");
var _rxError = require("../../rx-error");
var _utils = require("../../plugins/utils");
var _workerHelpers = require("./worker-helpers");
/**
 * This file contains the RxStorage implementation for Worker
 */

/**
 * Default settings for the worker storage
 */

/**
 * Default checkpoint type
 */

/**
 * Cache for the Worker instances
 * so we do not create multiple instances for the same worker
 */
var WORKER_CACHE = new Map();

/**
 * Creates a RxStorage that uses a Worker
 */
function getRxStorageWorker(settings) {
  // Check if Worker is available
  if (!(0, _workerHelpers.hasWorker)()) {
    throw (0, _rxError.newRxError)('UT4', {
      adapter: 'Worker is not available in this environment'
    });
  }
  return {
    name: 'worker',
    rxdbVersion: '14.0.0',
    async createStorageInstance(params) {
      var {
        databaseName,
        collectionName,
        schema,
        options = {},
        devMode,
        databaseInstanceToken,
        multiInstance
      } = params;

      // Get or create the Worker instance
      var workerKey = typeof settings.workerInput === 'string' ? settings.workerInput : 'function-worker';
      var worker = (0, _utils.getFromMapOrCreate)(WORKER_CACHE, workerKey, () => (0, _workerHelpers.createWorker)(settings.workerInput, settings.workerOptions));

      // Create the storage instance in the worker
      var instanceId = databaseName + '-' + collectionName;
      await (0, _workerHelpers.sendMessageToWorker)(worker, {
        type: 'create',
        id: (0, _workerHelpers.createMessageId)(),
        databaseName,
        collectionName,
        schema,
        options,
        devMode,
        multiInstance
      });

      // Create the storage instance
      var storageInstance = {
        databaseName,
        collectionName,
        schema: schema,
        internals: {},
        options,
        bulkWrite(documentWrites, context) {
          return (0, _workerHelpers.sendMessageToWorker)(worker, {
            type: 'bulkPut',
            id: (0, _workerHelpers.createMessageId)(),
            instanceId,
            documentWrites,
            context: context || ''
          });
        },
        findDocumentsById(documentIds, withDeleted) {
          return (0, _workerHelpers.sendMessageToWorker)(worker, {
            type: 'bulkGet',
            id: (0, _workerHelpers.createMessageId)(),
            instanceId,
            documentIds,
            withDeleted
          });
        },
        query(preparedQuery) {
          return (0, _workerHelpers.sendMessageToWorker)(worker, {
            type: 'query',
            id: (0, _workerHelpers.createMessageId)(),
            instanceId,
            preparedQuery
          });
        },
        count(preparedQuery) {
          return (0, _workerHelpers.sendMessageToWorker)(worker, {
            type: 'count',
            id: (0, _workerHelpers.createMessageId)(),
            instanceId,
            preparedQuery
          });
        },
        getAttachmentData(documentId, attachmentId, digest) {
          return (0, _workerHelpers.sendMessageToWorker)(worker, {
            type: 'getAttachment',
            id: (0, _workerHelpers.createMessageId)(),
            instanceId,
            documentId,
            attachmentId,
            digest
          });
        },
        cleanup(minimumDeletedTime) {
          return (0, _workerHelpers.sendMessageToWorker)(worker, {
            type: 'cleanup',
            id: (0, _workerHelpers.createMessageId)(),
            instanceId,
            minimumDeletedTime
          });
        },
        async close() {
          await (0, _workerHelpers.sendMessageToWorker)(worker, {
            type: 'close',
            id: (0, _workerHelpers.createMessageId)(),
            instanceId
          });
          return _utils.PROMISE_RESOLVE_VOID;
        },
        remove() {
          return (0, _workerHelpers.sendMessageToWorker)(worker, {
            type: 'remove',
            id: (0, _workerHelpers.createMessageId)(),
            instanceId
          });
        },
        changeStream() {
          var subject = new _rxjs.Subject();
          var id = (0, _workerHelpers.createMessageId)();
          var changeStreamHandler = event => {
            var data = event.data;
            if (data.type === 'changeStream' && data.id === id) {
              // 创建一个EventBulk对象，包含单个changeEvent
              var eventBulk = {
                id: (0, _workerHelpers.createMessageId)(),
                events: data.changeEvent ? [data.changeEvent] : [],
                checkpoint: {},
                context: ''
              };
              subject.next(eventBulk);
            }
          };
          worker.addEventListener('message', changeStreamHandler);

          // Start the change stream in the worker
          (0, _workerHelpers.sendMessageToWorker)(worker, {
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
              worker.removeEventListener('message', changeStreamHandler);
              // Stop the change stream in the worker
              (0, _workerHelpers.sendMessageToWorker)(worker, {
                type: 'unsubscribeChangeStream',
                id: (0, _workerHelpers.createMessageId)(),
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
//# sourceMappingURL=rx-storage-worker.js.map