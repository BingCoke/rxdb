/**
 * This file contains the RxStorage implementation for SharedWorker
 */

import { Subject } from 'rxjs';
import { newRxError } from '../../rx-error';
import { getFromMapOrCreate, PROMISE_RESOLVE_VOID } from '../../plugins/utils';
import { createMessageId, createSharedWorker, hasSharedWorker, sendMessageToWorker } from './shared-worker-helpers';

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
export function getRxStorageSharedWorker(settings) {
  // Check if SharedWorker is available
  if (!hasSharedWorker()) {
    throw newRxError('UT4', {
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
      var workerData = getFromMapOrCreate(SHARED_WORKER_CACHE, workerKey, () => {
        var worker = createSharedWorker(settings.workerInput, settings.workerOptions);
        var port = worker.port;
        port.start();
        return {
          worker,
          port
        };
      });

      // Create the storage instance in the worker
      var instanceId = databaseName + '-' + collectionName;
      await sendMessageToWorker(workerData.port, {
        type: 'create',
        id: createMessageId(),
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
          return sendMessageToWorker(workerData.port, {
            type: 'bulkPut',
            id: createMessageId(),
            instanceId,
            documentWrites,
            context: context || ''
          });
        },
        findDocumentsById(documentIds, withDeleted) {
          return sendMessageToWorker(workerData.port, {
            type: 'bulkGet',
            id: createMessageId(),
            instanceId,
            documentIds,
            withDeleted
          });
        },
        query(preparedQuery) {
          return sendMessageToWorker(workerData.port, {
            type: 'query',
            id: createMessageId(),
            instanceId,
            preparedQuery
          });
        },
        count(preparedQuery) {
          return sendMessageToWorker(workerData.port, {
            type: 'count',
            id: createMessageId(),
            instanceId,
            preparedQuery
          });
        },
        getAttachmentData(documentId, attachmentId, digest) {
          return sendMessageToWorker(workerData.port, {
            type: 'getAttachment',
            id: createMessageId(),
            instanceId,
            documentId,
            attachmentId,
            digest
          });
        },
        cleanup(minimumDeletedTime) {
          return sendMessageToWorker(workerData.port, {
            type: 'cleanup',
            id: createMessageId(),
            instanceId,
            minimumDeletedTime
          });
        },
        async close() {
          await sendMessageToWorker(workerData.port, {
            type: 'close',
            id: createMessageId(),
            instanceId
          });
          return PROMISE_RESOLVE_VOID;
        },
        remove() {
          return sendMessageToWorker(workerData.port, {
            type: 'remove',
            id: createMessageId(),
            instanceId
          });
        },
        changeStream() {
          var subject = new Subject();
          var id = createMessageId();
          var changeStreamHandler = event => {
            var data = event.data;
            if (data.type === 'changeStream' && data.id === id) {
              var changeEvent = data.changeEvent;
              subject.next(changeEvent);
            }
          };
          workerData.port.addEventListener('message', changeStreamHandler);

          // Start the change stream in the worker
          sendMessageToWorker(workerData.port, {
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
              sendMessageToWorker(workerData.port, {
                type: 'unsubscribeChangeStream',
                id: createMessageId(),
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