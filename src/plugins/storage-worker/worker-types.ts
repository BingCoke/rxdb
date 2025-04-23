/**
 * Types for the worker storage
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
 * Default settings for the worker storage
 */
export type RxStorageWorkerSettings = {
    /**
     * Path to the worker file
     * This must be a file that is reachable from the webserver
     */
    workerInput: string | (() => Worker);

    /**
     * Options for the Worker constructor
     * @link https://developer.mozilla.org/en-US/docs/Web/API/Worker/Worker
     */
    workerOptions?: WorkerOptions;
};

/**
 * Message types for communication between main thread and worker
 */
export type WorkerMessageType = 
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
export interface WorkerMessage {
    type: WorkerMessageType;
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
export interface WorkerResponseMessage extends WorkerMessage {
    result?: any;
    error?: {
        name: string;
        message: string;
        stack?: string;
    };
    changeEvent?: RxStorageChangeEvent<any>;
}
