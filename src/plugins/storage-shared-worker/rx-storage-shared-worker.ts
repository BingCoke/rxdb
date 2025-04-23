/**
 * This file contains the RxStorage implementation for SharedWorker
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
    RxAttachmentData,
    EventBulk,
    PreparedQuery,
    RxStorageBulkWriteResponse,
    RxStorageQueryResult,
    RxStorageCountResult
} from '../../types';
import { Observable, Subject } from 'rxjs';
import { newRxError } from '../../rx-error';
import {
    getFromMapOrCreate,
    PROMISE_RESOLVE_VOID
} from '../../plugins/utils';
import { 
    RxStorageSharedWorkerSettings,
    RxConflictResolutionStrategy,
    RxConflictResolutionTask
} from './shared-worker-types';
import { 
    createMessageId, 
    createSharedWorker, 
    hasSharedWorker, 
    sendMessageToWorker 
} from './shared-worker-helpers';

/**
 * Default settings for the shared worker storage
 */
export type RxStorageDefaultSettings = {
    /**
     * Any additional options
     */
    [key: string]: any;
};

/**
 * Default checkpoint type
 */
export type RxStorageDefaultCheckpoint = {
    /**
     * Any checkpoint data
     */
    [key: string]: any;
};

/**
 * Cache for the SharedWorker instances
 * so we do not create multiple instances for the same worker
 */
const SHARED_WORKER_CACHE = new Map<string, {
    worker: SharedWorker;
    port: MessagePort;
}>();

/**
 * Creates a RxStorage that uses a SharedWorker
 */
export function getRxStorageSharedWorker(
    settings: RxStorageSharedWorkerSettings
): RxStorage<RxStorageDefaultSettings, RxStorageDefaultCheckpoint> {
    // Check if SharedWorker is available
    if (!hasSharedWorker()) {
        throw newRxError('UT4', {
            adapter: 'SharedWorker is not available in this environment'
        });
    }

    const multiInstance = settings.multiInstance === false ? false : true;

    return {
        name: 'shared-worker',
        rxdbVersion: '14.0.0',
        async createStorageInstance<RxDocType>(
            params: RxStorageInstanceCreationParams<RxDocType, RxStorageDefaultSettings>
        ): Promise<RxStorageInstance<RxDocType, RxStorageDefaultSettings, RxStorageDefaultCheckpoint>> {
            const { 
                databaseName, 
                collectionName, 
                schema, 
                options = {},
                devMode,
                databaseInstanceToken,
                multiInstance: instanceMultiInstance
            } = params;

            // Get or create the SharedWorker instance
            const workerKey = typeof settings.workerInput === 'string' ? 
                settings.workerInput : 
                'function-worker';
            
            const workerData = getFromMapOrCreate(
                SHARED_WORKER_CACHE,
                workerKey,
                () => {
                    const worker = createSharedWorker(
                        settings.workerInput,
                        settings.workerOptions
                    );
                    const port = worker.port;
                    port.start();
                    return {
                        worker,
                        port
                    };
                }
            );

            // Create the storage instance in the worker
            const instanceId = databaseName + '-' + collectionName;
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
            const storageInstance: RxStorageInstance<RxDocType, RxStorageDefaultSettings, RxStorageDefaultCheckpoint> = {
                databaseName,
                collectionName,
                schema: schema as RxJsonSchema<RxDocumentData<RxDocType>>,
                internals: {},
                options,
                bulkWrite(
                    documentWrites: BulkWriteRow<RxDocType>[],
                    context?: string
                ): Promise<RxStorageBulkWriteResponse<RxDocType>> {
                    return sendMessageToWorker(workerData.port, {
                        type: 'bulkPut',
                        id: createMessageId(),
                        instanceId,
                        documentWrites,
                        context: context || ''
                    });
                },
                findDocumentsById(
                    documentIds: string[],
                    withDeleted: boolean
                ): Promise<RxDocumentData<RxDocType>[]> {
                    return sendMessageToWorker(workerData.port, {
                        type: 'bulkGet',
                        id: createMessageId(),
                        instanceId,
                        documentIds,
                        withDeleted
                    });
                },
                query(
                    preparedQuery: PreparedQuery<RxDocType>
                ): Promise<RxStorageQueryResult<RxDocType>> {
                    return sendMessageToWorker(workerData.port, {
                        type: 'query',
                        id: createMessageId(),
                        instanceId,
                        preparedQuery
                    });
                },
                count(
                    preparedQuery: PreparedQuery<RxDocType>
                ): Promise<RxStorageCountResult> {
                    return sendMessageToWorker(workerData.port, {
                        type: 'count',
                        id: createMessageId(),
                        instanceId,
                        preparedQuery
                    });
                },
                getAttachmentData(
                    documentId: string,
                    attachmentId: string,
                    digest: string
                ): Promise<string> {
                    return sendMessageToWorker(workerData.port, {
                        type: 'getAttachment',
                        id: createMessageId(),
                        instanceId,
                        documentId,
                        attachmentId,
                        digest
                    });
                },
                cleanup(
                    minimumDeletedTime: number
                ): Promise<boolean> {
                    return sendMessageToWorker(workerData.port, {
                        type: 'cleanup',
                        id: createMessageId(),
                        instanceId,
                        minimumDeletedTime
                    });
                },
                async close(): Promise<void> {
                    await sendMessageToWorker(workerData.port, {
                        type: 'close',
                        id: createMessageId(),
                        instanceId
                    });
                    return PROMISE_RESOLVE_VOID;
                },
                remove(): Promise<void> {
                    return sendMessageToWorker(workerData.port, {
                        type: 'remove',
                        id: createMessageId(),
                        instanceId
                    });
                },
                changeStream(): Observable<EventBulk<RxStorageChangeEvent<RxDocType>, any>> {
                    const subject = new Subject<EventBulk<RxStorageChangeEvent<RxDocType>, any>>();
                    
                    const id = createMessageId();
                    const changeStreamHandler = (event: MessageEvent) => {
                        const data = event.data;
                        if (data.type === 'changeStream' && data.id === id) {
                            const changeEvent = data.changeEvent as EventBulk<RxStorageChangeEvent<RxDocType>, any>;
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
                    const originalSubscribe = subject.subscribe.bind(subject);
                    subject.subscribe = function() {
                        const subscription = originalSubscribe.apply(this, arguments as any);
                        const originalUnsubscribe = subscription.unsubscribe.bind(subscription);
                        
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
