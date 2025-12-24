import type { RxJsonSchema, RxStorageInstanceCreationParams, RxStorageInstance, EventBulk, RxStorageChangeEvent, RxDocumentData, BulkWriteRow, RxStorageBulkWriteResponse, RxStorageQueryResult, StringKeys, RxStorageDefaultCheckpoint, RxStorageCountResult, RxStorage } from '../../types/index.d.ts';
import { BehaviorSubject, Observable } from 'rxjs';
import type { SQLiteJSONInstanceCreationOptions, SQLiteJSONInternals, SQLiteQueryWithParams, SQLiteJSONStorageSettings, ExtendedPreparedQuery, SQLiteBasics } from './sqlite-json-types.ts';
export * from './sqlite-json-helpers.ts';
export * from './sqlite-json-types.ts';
export * from './mongo-query-to-sql.ts';
export declare class RxStorageSQLiteJSON implements RxStorage<SQLiteJSONInternals, SQLiteJSONInstanceCreationOptions> {
    settings: SQLiteJSONStorageSettings;
    name: string;
    readonly rxdbVersion = "17.0.0-beta.2";
    constructor(settings: SQLiteJSONStorageSettings);
    /**
     * 创建存储实例
     */
    createStorageInstance<RxDocType>(params: RxStorageInstanceCreationParams<RxDocType, SQLiteJSONInstanceCreationOptions>): Promise<any>;
}
/**
 * 获取SQLite JSON存储实例
 */
export declare function getRxStorageSQLiteJSON(settings: SQLiteJSONStorageSettings): RxStorageSQLiteJSON;
/**
 * SQLite JSON存储实例
 * 利用SQLite的JSON功能实现高效的RxStorage
 */
export declare class RxStorageInstanceSQLiteJSON<RxDocType> implements RxStorageInstance<RxDocType, SQLiteJSONInternals, SQLiteJSONInstanceCreationOptions, RxStorageDefaultCheckpoint> {
    readonly storage: RxStorageSQLiteJSON;
    readonly databaseName: string;
    readonly collectionName: string;
    readonly schema: Readonly<RxJsonSchema<RxDocumentData<RxDocType>>>;
    readonly internals: SQLiteJSONInternals;
    readonly options: Readonly<SQLiteJSONInstanceCreationOptions>;
    readonly settings: SQLiteJSONStorageSettings;
    readonly tableName: string;
    readonly devMode: boolean;
    readonly primaryPath: StringKeys<RxDocType>;
    private changes$;
    readonly instanceId: number;
    closed?: Promise<void>;
    sqliteBasics: SQLiteBasics<any>;
    readonly openWriteCount$: BehaviorSubject<number>;
    constructor(storage: RxStorageSQLiteJSON, databaseName: string, collectionName: string, schema: Readonly<RxJsonSchema<RxDocumentData<RxDocType>>>, internals: SQLiteJSONInternals, options: Readonly<SQLiteJSONInstanceCreationOptions>, settings: SQLiteJSONStorageSettings, tableName: string, devMode: boolean);
    /**
     * 执行SQL查询，不返回结果
     */
    run(db: any, queryWithParams: SQLiteQueryWithParams): Promise<void>;
    /**
     * 执行SQL查询，返回结果
     */
    all(db: any, queryWithParams: SQLiteQueryWithParams): Promise<any[]>;
    /**
     * 批量写入文档
     */
    bulkWrite(documentWrites: BulkWriteRow<RxDocType>[], context: string): Promise<RxStorageBulkWriteResponse<RxDocType>>;
    /**
     * 将Mango查询转换为SQLite JSON查询
     * 利用SQLite的JSON函数高效查询嵌套数据
     */
    private mangoQueryToSQLiteJSONQuery;
    /**
     * 查询文档
     */
    /**
     * 打印查询信息(包含EXPLAIN结果)
     */
    private logQueryInfo;
    query(preparedQuery: ExtendedPreparedQuery<RxDocType>): Promise<RxStorageQueryResult<RxDocType>>;
    /**
     * 计数查询
     */
    count(preparedQuery: ExtendedPreparedQuery<RxDocType>): Promise<RxStorageCountResult>;
    /**
     * 根据ID查找文档
     */
    findDocumentsById(ids: string[], withDeleted: boolean): Promise<RxDocumentData<RxDocType>[]>;
    /**
     * 变更流
     */
    changeStream(): Observable<EventBulk<RxStorageChangeEvent<RxDocumentData<RxDocType>>, RxStorageDefaultCheckpoint>>;
    /**
     * 清理已删除的文档
     */
    cleanup(minimumDeletedTime: number): Promise<boolean>;
    /**
     * 获取附件数据
     * 当前实现不支持附件
     */
    getAttachmentData(_documentId: string, _attachmentId: string): Promise<string>;
    /**
     * 删除集合
     */
    remove(): Promise<void>;
    /**
     * 关闭存储实例
     */
    close(): Promise<void>;
}
/**
 * 创建SQLite JSON存储实例
 */
export declare function createSQLiteJSONStorageInstance<RxDocType>(storage: RxStorageSQLiteJSON, params: RxStorageInstanceCreationParams<RxDocType, SQLiteJSONInstanceCreationOptions>, settings: SQLiteJSONStorageSettings): Promise<RxStorageInstanceSQLiteJSON<RxDocType>>;
