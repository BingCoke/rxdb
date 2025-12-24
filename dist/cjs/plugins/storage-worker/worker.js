"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.exposeWorkerRxStorage = exposeWorkerRxStorage;
/**
 * This file is the implementation of the Worker
 * It handles messages from the main thread and manages the storage
 */

// Store instances by instanceId
var STORAGE_INSTANCES = new Map();

// Store change stream subscriptions
var CHANGE_STREAM_SUBSCRIPTIONS = new Map();

/**
 * Handle messages from the main thread
 */
function handleMessage(event, baseStorage) {
  var message = event.data;

  // Process the message asynchronously
  processMessage(message, baseStorage).catch(error => {
    // Send error back to main thread
    var response = {
      type: message.type,
      id: message.id,
      error: {
        name: error.name || 'Error',
        message: error.message || String(error),
        stack: error.stack
      }
    };
    // @ts-ignore - self is available in worker context
    self.postMessage(response);
  });
}

/**
 * Process a message from the main thread
 */
async function processMessage(message, baseStorage) {
  var result;
  switch (message.type) {
    case 'create':
      {
        // Create a new storage instance
        var params = {
          databaseName: message.databaseName,
          collectionName: message.collectionName,
          schema: message.schema,
          options: message.options || {},
          devMode: message.devMode === true,
          databaseInstanceToken: message.databaseName + '-token',
          multiInstance: message.multiInstance === false ? false : true
        };
        var instanceId = params.databaseName + '-' + params.collectionName;
        var storageInstance = await baseStorage.createStorageInstance(params);
        STORAGE_INSTANCES.set(instanceId, storageInstance);
        result = true;
        break;
      }
    case 'close':
      {
        // Close a storage instance
        var _instanceId = message.instanceId;
        var instance = STORAGE_INSTANCES.get(_instanceId);
        if (instance) {
          await instance.close();
          STORAGE_INSTANCES.delete(_instanceId);

          // Unsubscribe any change streams for this instance
          for (var [subscriptionId, subscription] of CHANGE_STREAM_SUBSCRIPTIONS.entries()) {
            if (subscription.instanceId === _instanceId) {
              subscription.unsubscribe();
              CHANGE_STREAM_SUBSCRIPTIONS.delete(subscriptionId);
            }
          }
        }
        result = true;
        break;
      }
    case 'get':
      {
        // Get a document by id
        var _instanceId2 = message.instanceId;
        var _instance = STORAGE_INSTANCES.get(_instanceId2);
        if (!_instance) {
          throw new Error("Storage instance not found: " + _instanceId2);
        }
        var docs = await _instance.findDocumentsById([message.documentId], true);
        result = docs.length > 0 ? docs[0] : undefined;
        break;
      }
    case 'bulkGet':
      {
        // Get multiple documents by id
        var _instanceId3 = message.instanceId;
        var _instance2 = STORAGE_INSTANCES.get(_instanceId3);
        if (!_instance2) {
          throw new Error("Storage instance not found: " + _instanceId3);
        }
        result = await _instance2.findDocumentsById(message.documentIds, true);
        break;
      }
    case 'bulkPut':
      {
        // Write multiple documents
        var _instanceId4 = message.instanceId;
        var _instance3 = STORAGE_INSTANCES.get(_instanceId4);
        if (!_instance3) {
          throw new Error("Storage instance not found: " + _instanceId4);
        }
        result = await _instance3.bulkWrite(message.documentWrites, message.context || '');
        break;
      }
    case 'bulkRemove':
      {
        // Remove multiple documents by marking them as deleted
        var _instanceId5 = message.instanceId;
        var _instance4 = STORAGE_INSTANCES.get(_instanceId5);
        if (!_instance4) {
          throw new Error("Storage instance not found: " + _instanceId5);
        }

        // First get the documents
        var _docs = await _instance4.findDocumentsById(message.documentIds, true);

        // Then mark them as deleted and write them back
        var writes = _docs.map(doc => ({
          previous: doc,
          document: {
            ...doc,
            _deleted: true
          }
        }));
        result = await _instance4.bulkWrite(writes, 'bulkRemove');
        break;
      }
    case 'query':
      {
        // Query documents
        var _instanceId6 = message.instanceId;
        var _instance5 = STORAGE_INSTANCES.get(_instanceId6);
        if (!_instance5) {
          throw new Error("Storage instance not found: " + _instanceId6);
        }
        result = await _instance5.query(message.preparedQuery);
        break;
      }
    case 'count':
      {
        // Count documents
        var _instanceId7 = message.instanceId;
        var _instance6 = STORAGE_INSTANCES.get(_instanceId7);
        if (!_instance6) {
          throw new Error("Storage instance not found: " + _instanceId7);
        }
        result = await _instance6.count(message.preparedQuery);
        break;
      }
    case 'getAll':
      {
        // Get all documents by querying with empty selector
        var _instanceId8 = message.instanceId;
        var _instance7 = STORAGE_INSTANCES.get(_instanceId8);
        if (!_instance7) {
          throw new Error("Storage instance not found: " + _instanceId8);
        }
        var queryResult = await _instance7.query({
          query: {
            selector: {},
            sort: [{
              _id: 'asc'
            }],
            skip: 0
          },
          queryPlan: {
            index: ['_id'],
            selectorSatisfiedByIndex: true,
            sortSatisfiedByIndex: true,
            startKeys: [],
            endKeys: [],
            inclusiveStart: true,
            inclusiveEnd: true
          }
        });
        result = queryResult.documents;
        break;
      }
    case 'getByIds':
      {
        // Get documents by ids
        var _instanceId9 = message.instanceId;
        var _instance8 = STORAGE_INSTANCES.get(_instanceId9);
        if (!_instance8) {
          throw new Error("Storage instance not found: " + _instanceId9);
        }
        result = await _instance8.findDocumentsById(message.ids, true);
        break;
      }
    case 'cleanup':
      {
        // Cleanup storage
        var _instanceId0 = message.instanceId;
        var _instance9 = STORAGE_INSTANCES.get(_instanceId0);
        if (!_instance9) {
          throw new Error("Storage instance not found: " + _instanceId0);
        }

        // Use a default minimum deleted time of 1 hour
        result = await _instance9.cleanup(60 * 60 * 1000);
        break;
      }
    case 'remove':
      {
        // Remove storage
        var _instanceId1 = message.instanceId;
        var _instance0 = STORAGE_INSTANCES.get(_instanceId1);
        if (!_instance0) {
          throw new Error("Storage instance not found: " + _instanceId1);
        }
        result = await _instance0.remove();
        STORAGE_INSTANCES.delete(_instanceId1);
        break;
      }
    case 'getAttachment':
      {
        // Get attachment data
        var _instanceId10 = message.instanceId;
        var _instance1 = STORAGE_INSTANCES.get(_instanceId10);
        if (!_instance1) {
          throw new Error("Storage instance not found: " + _instanceId10);
        }
        result = await _instance1.getAttachmentData(message.documentId, message.attachmentId, message.digest);
        break;
      }
    case 'changeStream':
      {
        // Subscribe to change stream
        var _instanceId11 = message.instanceId;
        var _instance10 = STORAGE_INSTANCES.get(_instanceId11);
        if (!_instance10) {
          throw new Error("Storage instance not found: " + _instanceId11);
        }
        var observable = _instance10.changeStream();
        var _subscription = observable.subscribe({
          next: eventBulk => {
            // For each event in the bulk, send a separate message
            eventBulk.events.forEach(changeEvent => {
              // Send change event to main thread
              var response = {
                type: 'changeStream',
                id: message.id,
                changeEvent
              };
              // @ts-ignore - self is available in worker context
              self.postMessage(response);
            });
          }
        });

        // Store the subscription
        CHANGE_STREAM_SUBSCRIPTIONS.set(message.id, {
          instanceId: _instanceId11,
          unsubscribe: _subscription.unsubscribe
        });
        result = true;
        break;
      }
    case 'unsubscribeChangeStream':
      {
        // Unsubscribe from change stream
        var _subscriptionId = message.subscriptionId;
        var _subscription2 = CHANGE_STREAM_SUBSCRIPTIONS.get(_subscriptionId);
        if (_subscription2) {
          _subscription2.unsubscribe();
          CHANGE_STREAM_SUBSCRIPTIONS.delete(_subscriptionId);
        }
        result = true;
        break;
      }
    default:
      throw new Error("Unknown message type: " + message.type);
  }

  // Send response back to main thread
  var response = {
    type: message.type,
    id: message.id,
    result
  };

  // @ts-ignore - self is available in worker context
  self.postMessage(response);
}

/**
 * Expose the RxStorage to the outside
 * This function should be called in the worker file
 */
function exposeWorkerRxStorage(options) {
  var baseStorage = options.storage;

  // Listen for messages from main thread
  // @ts-ignore - self is available in worker context
  self.addEventListener('message', event => {
    handleMessage(event, baseStorage);
  });
}
//# sourceMappingURL=worker.js.map