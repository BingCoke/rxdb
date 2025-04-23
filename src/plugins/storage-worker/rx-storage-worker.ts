/**
 * This file contains the RxStorage implementation for Worker
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
    RxStorageWorkerSettings
} from './worker-types';
import { 
    createMessageId, 
    createWorker, 
    hasWorker, 
    sendMessageToWorker 
} from './worker-helpers';

/**
 * Default settings for the worker storage
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
 * Cache for the Worker instances
 * so we do not create multiple instances for the same worker
 */
const WORKER_CACHE = new Map<string, Worker>();

/**
 * Creates a RxStorage that uses a Worker
 */
export function getRxStorageWorker(
    settings: RxStorageWorkerSettings
): RxStorage<RxStorageDefaultSettings, RxStorageDefaultCheckpoint> {
    // Check if Worker is available
    if (!hasWorker()) {
        throw newRxError('UT4', {
            adapter: 'Worker is not available in this environment'
        });
    }

    return {
        name: 'worker',
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
                multiInstance
            } = params;

            // Get or create the Worker instance
            const workerKey = typeof settings.workerInput === 'string' ? 
                settings.workerInput : 
                'function-worker';
            
            const worker = getFromMapOrCreate(
                WORKER_CACHE,
                workerKey,
                () => createWorker(
                    settings.workerInput,
                    settings.workerOptions
                )
            );

            // Create the storage instance in the worker
            const instanceId = databaseName + '-' + collectionName;
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
                    return sendMessageToWorker(worker, {
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
                    return sendMessageToWorker(worker, {
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
                    return sendMessageToWorker(worker, {
                        type: 'query',
                        id: createMessageId(),
                        instanceId,
                        preparedQuery
                    });
                },
                count(
                    preparedQuery: PreparedQuery<RxDocType>
                ): Promise<RxStorageCountResult> {
                    return sendMessageToWorker(worker, {
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
                    return sendMessageToWorker(worker, {
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
                    return sendMessageToWorker(worker, {
                        type: 'cleanup',
                        id: createMessageId(),
                        instanceId,
                        minimumDeletedTime
                    });
                },
                async close(): Promise<void> {
                    await sendMessageToWorker(worker, {
                        type: 'close',
                        id: createMessageId(),
                        instanceId
                    });
                    return PROMISE_RESOLVE_VOID;
                },
                remove(): Promise<void> {
                    return sendMessageToWorker(worker, {
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
                            // 创建一个EventBulk对象，包含单个changeEvent
                            const eventBulk: EventBulk<RxStorageChangeEvent<RxDocType>, any> = {
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
                    const originalSubscribe = subject.subscribe.bind(subject);
                    subject.subscribe = function() {
                        const subscription = originalSubscribe.apply(this, arguments as any);
                        const originalUnsubscribe = subscription.unsubscribe.bind(subscription);
                        
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
