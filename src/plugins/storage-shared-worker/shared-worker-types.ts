/**
 * Types for the shared worker storage
 */
import type {
    RxStorage,
    RxStorageInstanceCreationParams,
    RxJsonSchema,
    RxDocumentData,
    RxStorageChangeEvent,
    BulkWriteRow,
    RxAttachmentWriteData
} from '../../types';

/**
 * Default settings for the shared worker storage
 */
export type RxStorageSharedWorkerSettings = {
    /**
     * Path to the shared worker file
     * This must be a file that is reachable from the webserver
     */
    workerInput: string | (() => SharedWorker);

    /**
     * Options for the SharedWorker constructor
     * @link https://developer.mozilla.org/en-US/docs/Web/API/SharedWorker/SharedWorker
     */
    workerOptions?: WorkerOptions;

    /**
     * If set to true, change events will not be shared
     * across JavaScript realms. This improves performance
     * but should only be used when you know that you only
     * ever create the RxDatabase once inside of the shared worker
     * and once on the main thread.
     * @default false
     */
    multiInstance?: boolean;
};

/**
 * Message types for communication between main thread and worker
 */
export type SharedWorkerMessageType = 
    | 'create'
    | 'close'
    | 'get'
    | 'getKeys'
    | 'bulkGet'
    | 'remove'
    | 'bulkRemove'
    | 'put'
    | 'bulkPut'
    | 'changeStream'
    | 'unsubscribeChangeStream'
    | 'cleanup'
    | 'query'
    | 'count'
    | 'getAll'
    | 'getByIds'
    | 'getAttachment'
    | 'getAttachmentBase64'
    | 'bulkAddAttachments';

/**
 * Base message interface
 */
export interface SharedWorkerMessage {
    type: SharedWorkerMessageType;
    id: string;
    instanceId?: string;
    databaseName?: string;
    collectionName?: string;
    schema?: RxJsonSchema<any>;
    options?: any;
    devMode?: boolean;
    multiInstance?: boolean;
    documentId?: string;
    documentIds?: string[];
    documentWrites?: BulkWriteRow<any>[];
    context?: string;
    preparedQuery?: any;
    ids?: string[];
    attachmentId?: string;
    digest?: string;
    attachmentsData?: RxAttachmentWriteData[];
    subscriptionId?: string;
    withDeleted?: boolean;
    minimumDeletedTime?: number;
}

/**
 * Response message from the worker
 */
export interface SharedWorkerResponseMessage extends SharedWorkerMessage {
    result?: any;
    error?: {
        name: string;
        message: string;
        stack?: string;
    };
    changeEvent?: RxStorageChangeEvent<any>;
}

/**
 * Conflict resolution strategy
 */
export enum RxConflictResolutionStrategy {
    LAST_WRITE_WINS = 'last-write-wins'
}

/**
 * Conflict resolution task
 */
export interface RxConflictResolutionTask<RxDocType> {
    documentId: string;
    newDocumentState: RxDocumentData<RxDocType>;
    realMasterState: RxDocumentData<RxDocType>;
}
