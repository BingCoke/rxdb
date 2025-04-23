/**
 * This file is the implementation of the Worker
 * It handles messages from the main thread and manages the storage
 */
import type {
    RxStorage,
    RxStorageInstance,
    RxStorageInstanceCreationParams,
    RxJsonSchema,
    RxDocumentData,
    RxStorageChangeEvent,
    BulkWriteRow,
    RxAttachmentWriteData,
    EventBulk,
    PreparedQuery
} from '../../types';
import { 
    WorkerMessage, 
    WorkerResponseMessage 
} from './worker-types';

// Store instances by instanceId
const STORAGE_INSTANCES = new Map<string, RxStorageInstance<any, any, any>>();

// Store change stream subscriptions
const CHANGE_STREAM_SUBSCRIPTIONS = new Map<string, { 
    instanceId: string, 
    unsubscribe: () => void 
}>();

/**
 * Handle messages from the main thread
 */
function handleMessage(
    event: MessageEvent<WorkerMessage>,
    baseStorage: RxStorage<any, any>
) {
    const message = event.data as WorkerMessage;
    
    // Process the message asynchronously
    processMessage(message, baseStorage).catch(error => {
        // Send error back to main thread
        const response: WorkerResponseMessage = {
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
async function processMessage(
    message: WorkerMessage,
    baseStorage: RxStorage<any, any>
): Promise<void> {
    let result: any;
    
    switch (message.type) {
        case 'create': {
            // Create a new storage instance
            const params: RxStorageInstanceCreationParams<any, any> = {
                databaseName: message.databaseName!,
                collectionName: message.collectionName!,
                schema: message.schema!,
                options: message.options || {},
                devMode: message.devMode === true,
                databaseInstanceToken: message.databaseName! + '-token',
                multiInstance: message.multiInstance === false ? false : true
            };
            
            const instanceId = params.databaseName + '-' + params.collectionName;
            const storageInstance = await baseStorage.createStorageInstance(params);
            STORAGE_INSTANCES.set(instanceId, storageInstance);
            
            result = true;
            break;
        }
        
        case 'close': {
            // Close a storage instance
            const instanceId = message.instanceId!;
            const instance = STORAGE_INSTANCES.get(instanceId);
            
            if (instance) {
                await instance.close();
                STORAGE_INSTANCES.delete(instanceId);
                
                // Unsubscribe any change streams for this instance
                for (const [subscriptionId, subscription] of CHANGE_STREAM_SUBSCRIPTIONS.entries()) {
                    if (subscription.instanceId === instanceId) {
                        subscription.unsubscribe();
                        CHANGE_STREAM_SUBSCRIPTIONS.delete(subscriptionId);
                    }
                }
            }
            
            result = true;
            break;
        }
        
        case 'get': {
            // Get a document by id
            const instanceId = message.instanceId!;
            const instance = STORAGE_INSTANCES.get(instanceId);
            
            if (!instance) {
                throw new Error(`Storage instance not found: ${instanceId}`);
            }
            
            const docs = await instance.findDocumentsById([message.documentId!], true);
            result = docs.length > 0 ? docs[0] : undefined;
            break;
        }
        
        case 'bulkGet': {
            // Get multiple documents by id
            const instanceId = message.instanceId!;
            const instance = STORAGE_INSTANCES.get(instanceId);
            
            if (!instance) {
                throw new Error(`Storage instance not found: ${instanceId}`);
            }
            
            result = await instance.findDocumentsById(message.documentIds!, true);
            break;
        }
        
        case 'bulkPut': {
            // Write multiple documents
            const instanceId = message.instanceId!;
            const instance = STORAGE_INSTANCES.get(instanceId);
            
            if (!instance) {
                throw new Error(`Storage instance not found: ${instanceId}`);
            }
            
            result = await instance.bulkWrite(message.documentWrites!, message.context || '');
            break;
        }
        
        case 'bulkRemove': {
            // Remove multiple documents by marking them as deleted
            const instanceId = message.instanceId!;
            const instance = STORAGE_INSTANCES.get(instanceId);
            
            if (!instance) {
                throw new Error(`Storage instance not found: ${instanceId}`);
            }
            
            // First get the documents
            const docs = await instance.findDocumentsById(message.documentIds!, true);
            
            // Then mark them as deleted and write them back
            const writes: BulkWriteRow<any>[] = docs.map(doc => ({
                previous: doc,
                document: {
                    ...doc,
                    _deleted: true
                }
            }));
            
            result = await instance.bulkWrite(writes, 'bulkRemove');
            break;
        }
        
        case 'query': {
            // Query documents
            const instanceId = message.instanceId!;
            const instance = STORAGE_INSTANCES.get(instanceId);
            
            if (!instance) {
                throw new Error(`Storage instance not found: ${instanceId}`);
            }
            
            result = await instance.query(message.preparedQuery!);
            break;
        }
        
        case 'count': {
            // Count documents
            const instanceId = message.instanceId!;
            const instance = STORAGE_INSTANCES.get(instanceId);
            
            if (!instance) {
                throw new Error(`Storage instance not found: ${instanceId}`);
            }
            
            result = await instance.count(message.preparedQuery!);
            break;
        }
        
        case 'getAll': {
            // Get all documents by querying with empty selector
            const instanceId = message.instanceId!;
            const instance = STORAGE_INSTANCES.get(instanceId);
            
            if (!instance) {
                throw new Error(`Storage instance not found: ${instanceId}`);
            }
            
            const queryResult = await instance.query({
                query: {
                    selector: {},
                    sort: [{ _id: 'asc' }],
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
        
        case 'getByIds': {
            // Get documents by ids
            const instanceId = message.instanceId!;
            const instance = STORAGE_INSTANCES.get(instanceId);
            
            if (!instance) {
                throw new Error(`Storage instance not found: ${instanceId}`);
            }
            
            result = await instance.findDocumentsById(message.ids!, true);
            break;
        }
        
        case 'cleanup': {
            // Cleanup storage
            const instanceId = message.instanceId!;
            const instance = STORAGE_INSTANCES.get(instanceId);
            
            if (!instance) {
                throw new Error(`Storage instance not found: ${instanceId}`);
            }
            
            // Use a default minimum deleted time of 1 hour
            result = await instance.cleanup(60 * 60 * 1000);
            break;
        }
        
        case 'remove': {
            // Remove storage
            const instanceId = message.instanceId!;
            const instance = STORAGE_INSTANCES.get(instanceId);
            
            if (!instance) {
                throw new Error(`Storage instance not found: ${instanceId}`);
            }
            
            result = await instance.remove();
            STORAGE_INSTANCES.delete(instanceId);
            break;
        }
        
        case 'getAttachment': {
            // Get attachment data
            const instanceId = message.instanceId!;
            const instance = STORAGE_INSTANCES.get(instanceId);
            
            if (!instance) {
                throw new Error(`Storage instance not found: ${instanceId}`);
            }
            
            result = await instance.getAttachmentData(
                message.documentId!,
                message.attachmentId!,
                message.digest!
            );
            break;
        }
        
        case 'changeStream': {
            // Subscribe to change stream
            const instanceId = message.instanceId!;
            const instance = STORAGE_INSTANCES.get(instanceId);
            
            if (!instance) {
                throw new Error(`Storage instance not found: ${instanceId}`);
            }
            
            const observable = instance.changeStream();
            const subscription = observable.subscribe({
                next: (eventBulk: EventBulk<RxStorageChangeEvent<any>, any>) => {
                    // For each event in the bulk, send a separate message
                    eventBulk.events.forEach(changeEvent => {
                        // Send change event to main thread
                        const response: WorkerResponseMessage = {
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
                instanceId,
                unsubscribe: subscription.unsubscribe
            });
            
            result = true;
            break;
        }
        
        case 'unsubscribeChangeStream': {
            // Unsubscribe from change stream
            const subscriptionId = message.subscriptionId!;
            const subscription = CHANGE_STREAM_SUBSCRIPTIONS.get(subscriptionId);
            
            if (subscription) {
                subscription.unsubscribe();
                CHANGE_STREAM_SUBSCRIPTIONS.delete(subscriptionId);
            }
            
            result = true;
            break;
        }
        
        default:
            throw new Error(`Unknown message type: ${message.type}`);
    }
    
    // Send response back to main thread
    const response: WorkerResponseMessage = {
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
export function exposeWorkerRxStorage(
    options: {
        storage: RxStorage<any, any>
    }
): void {
    const baseStorage = options.storage;
    
    // Listen for messages from main thread
    // @ts-ignore - self is available in worker context
    self.addEventListener('message', (event: MessageEvent<WorkerMessage>) => {
        handleMessage(event, baseStorage);
    });
}
