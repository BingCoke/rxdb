/**
 * This file contains the RxStorage implementation for Worker
 */

import { Subject } from 'rxjs';
import { newRxError } from '../../rx-error';
import { getFromMapOrCreate, PROMISE_RESOLVE_VOID } from '../../plugins/utils';
import { createMessageId, createWorker, hasWorker, sendMessageToWorker } from './worker-helpers';

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
export function getRxStorageWorker(settings) {
  // Check if Worker is available
  if (!hasWorker()) {
    throw newRxError('UT4', {
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
      var worker = getFromMapOrCreate(WORKER_CACHE, workerKey, () => createWorker(settings.workerInput, settings.workerOptions));

      // Create the storage instance in the worker
      var instanceId = databaseName + '-' + collectionName;
      await sendMessageToWorker(worker, {
        type: 'create',
        id: createMessageId(),
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
          return sendMessageToWorker(worker, {
            type: 'bulkPut',
            id: createMessageId(),
            instanceId,
            documentWrites,
            context: context || ''
          });
        },
        findDocumentsById(documentIds, withDeleted) {
          return sendMessageToWorker(worker, {
            type: 'bulkGet',
            id: createMessageId(),
            instanceId,
            documentIds,
            withDeleted
          });
        },
        query(preparedQuery) {
          return sendMessageToWorker(worker, {
            type: 'query',
            id: createMessageId(),
            instanceId,
            preparedQuery
          });
        },
        count(preparedQuery) {
          return sendMessageToWorker(worker, {
            type: 'count',
            id: createMessageId(),
            instanceId,
            preparedQuery
          });
        },
        getAttachmentData(documentId, attachmentId, digest) {
          return sendMessageToWorker(worker, {
            type: 'getAttachment',
            id: createMessageId(),
            instanceId,
            documentId,
            attachmentId,
            digest
          });
        },
        cleanup(minimumDeletedTime) {
          return sendMessageToWorker(worker, {
            type: 'cleanup',
            id: createMessageId(),
            instanceId,
            minimumDeletedTime
          });
        },
        async close() {
          await sendMessageToWorker(worker, {
            type: 'close',
            id: createMessageId(),
            instanceId
          });
          return PROMISE_RESOLVE_VOID;
        },
        remove() {
          return sendMessageToWorker(worker, {
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
              // 创建一个EventBulk对象，包含单个changeEvent
              var eventBulk = {
                id: createMessageId(),
                events: data.changeEvent ? [data.changeEvent] : [],
                checkpoint: {},
                context: ''
              };
              subject.next(eventBulk);
            }
          };
          worker.addEventListener('message', changeStreamHandler);

          // Start the change stream in the worker
          sendMessageToWorker(worker, {
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
              sendMessageToWorker(worker, {
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
//# sourceMappingURL=rx-storage-worker.js.map