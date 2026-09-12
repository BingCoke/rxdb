import {
    getPrimaryFieldOfPrimaryKey,
    categorizeBulkWriteRows,
    ensureNotFalsy,
    addRxStorageMultiInstanceSupport,
    getQueryMatcher,
    getSortComparator,
    newRxError,
    ensureRxStorageInstanceParamsAreCorrect,
    RXDB_VERSION
} from '../../index.ts';

import type {
    RxJsonSchema,
    RxStorageInstanceCreationParams,
    RxStorageInstance,
    EventBulk,
    RxStorageChangeEvent,
    RxDocumentData,
    BulkWriteRow,
    RxStorageBulkWriteResponse,
    RxStorageQueryResult,
    StringKeys,
    RxStorageDefaultCheckpoint,
    CategorizeBulkWriteRowsOutput,
    RxStorageCountResult,
    RxStorage,
    PreparedQuery,
} from '../../types/index.d.ts';
import { BehaviorSubject, Observable, Subject } from 'rxjs';
import {
    ensureParamsCountIsCorrect,
    getDatabaseConnection,
    getSQLiteJSONConnectionLease,
    getSQLiteJSONUpdateSQL,
    RX_STORAGE_NAME_SQLITE_JSON,
    getDataFromResultRow,
    getSQLiteJSONInsertSQL,
    createJsonIndexSQL,
    createMultiKeyIndexTableSQL,
    getMultiKeyIndexInsertSQL,
    getMultiKeyIndexDeleteSQL,
    getNestedValue,
    dropMultiKeyIndexTableSQL,
    type SQLiteJSONOperations
} from './sqlite-json-helpers.ts';
import {
    createMongoQuerySQLConverter
} from './mongo-query-to-sql.ts';
import type {
    SQLiteJSONInstanceCreationOptions,
    SQLiteJSONInternals,
    SQLiteQueryWithParams,
    SQLiteJSONStorageSettings,
    ExtendedPreparedQuery,
    SQLiteBasics
} from './sqlite-json-types.ts';
export * from './sqlite-json-helpers.ts';
export * from './sqlite-json-types.ts';
export * from './mongo-query-to-sql.ts';



export class RxStorageSQLiteJSON implements RxStorage<SQLiteJSONInternals, SQLiteJSONInstanceCreationOptions> {
    public name = RX_STORAGE_NAME_SQLITE_JSON;
    readonly rxdbVersion = RXDB_VERSION;

    constructor(
        public settings: SQLiteJSONStorageSettings
    ) { }

    /**
     * 创建存储实例
     */
    public async createStorageInstance<RxDocType>(
        params: RxStorageInstanceCreationParams<RxDocType, SQLiteJSONInstanceCreationOptions>
    ): Promise<any> {
        ensureRxStorageInstanceParamsAreCorrect(params);
        return createSQLiteJSONStorageInstance(this, params, this.settings);
    }

    /**
     * 获取原生SQLite数据库实例，用于执行原生SQL查询
     */
    async getSQLiteDatabase(databaseName: string): Promise<any> {
        const useDatabaseName = (this.settings.databaseNamePrefix ? this.settings.databaseNamePrefix : '') + '_' + databaseName;
        return getDatabaseConnection(this.settings.sqliteBasics, useDatabaseName);
    }
}

/**
 * 获取SQLite JSON存储实例
 */
export function getRxStorageSQLiteJSON(
    settings: SQLiteJSONStorageSettings
): RxStorageSQLiteJSON {
    const storage = new RxStorageSQLiteJSON(settings);
    return storage;
}


let instanceId = 0;

type SQLiteJSONQueryPlan<RxDocType> = {
    query: SQLiteQueryWithParams;
    fallback?: {
        matches: (doc: RxDocumentData<RxDocType>) => boolean;
        compare: (a: RxDocumentData<RxDocType>, b: RxDocumentData<RxDocType>) => number;
        skip: number;
        limit: number;
    };
};

/**
 * SQLite JSON存储实例
 * 利用SQLite的JSON功能实现高效的RxStorage
 */
export class RxStorageInstanceSQLiteJSON<RxDocType> implements RxStorageInstance<
    RxDocType,
    SQLiteJSONInternals,
    SQLiteJSONInstanceCreationOptions,
    RxStorageDefaultCheckpoint
> {
    public readonly primaryPath: StringKeys<RxDocType>;
    private changes$: Subject<EventBulk<RxStorageChangeEvent<RxDocumentData<RxDocType>>, RxStorageDefaultCheckpoint>> = new Subject();
    public readonly instanceId = instanceId++;
    public closed?: Promise<void>;

    public sqliteBasics: SQLiteBasics<any>;

    public readonly openWriteCount$ = new BehaviorSubject(0);

    private readonly arrayFields: Set<string>;
    private readonly admittedOperations = new Set<Promise<any>>();

    constructor(
        public readonly storage: RxStorageSQLiteJSON,
        public readonly databaseName: string,
        public readonly collectionName: string,
        public readonly schema: Readonly<RxJsonSchema<RxDocumentData<RxDocType>>>,
        public readonly internals: SQLiteJSONInternals,
        public readonly options: Readonly<SQLiteJSONInstanceCreationOptions>,
        public readonly settings: SQLiteJSONStorageSettings,
        public readonly tableName: string,
        public readonly devMode: boolean
    ) {
        this.sqliteBasics = storage.settings.sqliteBasics;
        this.primaryPath = getPrimaryFieldOfPrimaryKey(this.schema.primaryKey) as any;
        this.arrayFields = extractArrayFieldsFromSchema(schema);
    }

    private admit<T>(operation: () => Promise<T>): Promise<T> {
        if (this.closed) {
            return Promise.reject(newRxError('SNH', { args: { reason: 'SQLite JSON storage instance is closed' } }));
        }
        const promise = Promise.resolve().then(operation);
        this.admittedOperations.add(promise);
        promise.finally(() => this.admittedOperations.delete(promise)).catch(() => { });
        return promise;
    }

    private async drainAdmittedOperations(): Promise<void> {
        while (this.admittedOperations.size > 0) {
            await Promise.allSettled(Array.from(this.admittedOperations));
        }
    }

    private run(
        operations: { run: (query: SQLiteQueryWithParams) => Promise<void> },
        queryWithParams: SQLiteQueryWithParams
    ): Promise<void> {
        if (this.devMode) {
            ensureParamsCountIsCorrect(queryWithParams);
        }
        return operations.run(queryWithParams);
    }

    private all(
        operations: { all: (query: SQLiteQueryWithParams) => Promise<any[]> },
        queryWithParams: SQLiteQueryWithParams
    ): Promise<any[]> {
        if (this.devMode) {
            ensureParamsCountIsCorrect(queryWithParams);
        }
        return operations.all(queryWithParams);
    }

    private async findDocumentsByIdWithOperations(
        operations: { all: (query: SQLiteQueryWithParams) => Promise<any[]> },
        ids: string[],
        withDeleted: boolean
    ): Promise<RxDocumentData<RxDocType>[]> {
        if (ids.length === 0) {
            return [];
        }
        const placeholders = ids.map(() => '?').join(', ');
        let query = `SELECT data FROM "${this.tableName}" WHERE id IN (${placeholders})`;
        if (!withDeleted) {
            query += ` AND deleted = 0`;
        }
        const result = await this.all(operations, {
            query,
            params: ids,
            context: { method: 'findDocumentsById', data: ids }
        });
        return result
            .map(row => {
                try {
                    const rowData = getDataFromResultRow(row);
                    return typeof rowData === 'string' ? JSON.parse(rowData) : rowData;
                } catch (err) {
                    return null;
                }
            })
            .filter((doc): doc is RxDocumentData<RxDocType> => doc !== null);
    }

    /**
     * 批量写入文档
     */
    async bulkWrite(
        documentWrites: BulkWriteRow<RxDocType>[],
        context: string
    ): Promise<RxStorageBulkWriteResponse<RxDocType>> {
        return this.admit(async () => {
            this.openWriteCount$.next(this.openWriteCount$.getValue() + 1);
            try {
                const lease = this.internals.connectionLease;
                const ret: RxStorageBulkWriteResponse<RxDocType> = {
                    error: []
                };
                let categorized: CategorizeBulkWriteRowsOutput<RxDocType> = {} as any;

                await lease.transaction(async operations => {

                    // 只查询需要操作的文档ID
                    const docIds = documentWrites.map(d => (d.document as any)[this.primaryPath]);

                    const result = await this.findDocumentsByIdWithOperations(operations, docIds, true);
                    // 执行查询

                    // 构建文档映射
                    const docsInDb: Map<string, RxDocumentData<RxDocType>> = new Map();
                    result.forEach(doc => {
                        const id = doc[this.primaryPath];
                        if (id) {
                            docsInDb.set(id as string, doc);
                        }
                    });

                    // 分类批量写入行
                    categorized = categorizeBulkWriteRows(
                        this,
                        this.primaryPath,
                        docsInDb,
                        documentWrites,
                        context
                    );
                    ret.error = categorized.errors;

                    // 执行插入操作
                    for (const row of categorized.bulkInsertDocs) {
                        const insertQuery = getSQLiteJSONInsertSQL(
                            this.tableName,
                            this.primaryPath as any,
                            row.document,
                        );
                        await this.run(operations, insertQuery);

                        // 维护多键索引 - 插入
                        const docId = row.document[this.primaryPath] as string;
                        for (const fieldPath of this.arrayFields) {
                            const arrayValue = getNestedValue(row.document, fieldPath);
                            if (Array.isArray(arrayValue) && arrayValue.length > 0) {
                                const mkiInsertQueries = getMultiKeyIndexInsertSQL(
                                    this.tableName,
                                    fieldPath,
                                    docId,
                                    arrayValue
                                );
                                for (const mkiQuery of mkiInsertQueries) {
                                    await this.run(operations, mkiQuery);
                                }
                            }
                        }
                    }

                    // 执行更新操作
                    for (const row of categorized.bulkUpdateDocs) {
                        const updateQuery = getSQLiteJSONUpdateSQL<RxDocType>(
                            this.tableName,
                            this.primaryPath,
                            row,
                        );
                        await this.run(operations, updateQuery);

                        // 维护多键索引 - 先删除旧条目，再插入新条目（仅对非删除文档）
                        const docId = row.document[this.primaryPath] as string;
                        for (const fieldPath of this.arrayFields) {
                            // 删除旧的索引条目
                            const mkiDeleteQuery = getMultiKeyIndexDeleteSQL(
                                this.tableName,
                                fieldPath,
                                docId
                            );
                            await this.run(operations, mkiDeleteQuery);

                            // 只有非删除文档才插入新的索引条目
                            if (!row.document._deleted) {
                                const arrayValue = getNestedValue(row.document, fieldPath);
                                if (Array.isArray(arrayValue) && arrayValue.length > 0) {
                                    const mkiInsertQueries = getMultiKeyIndexInsertSQL(
                                        this.tableName,
                                        fieldPath,
                                        docId,
                                        arrayValue
                                    );
                                    for (const mkiQuery of mkiInsertQueries) {
                                        await this.run(operations, mkiQuery);
                                    }
                                }
                            }
                        }
                    }

                }, () => 'COMMIT', {
                    databaseName: this.databaseName,
                    collectionName: this.collectionName,
                    context
                });

                // 发送变更事件
                if (categorized.eventBulk.events.length > 0) {
                    const lastState = ensureNotFalsy(categorized.newestRow).document;
                    categorized.eventBulk.checkpoint = {
                        id: lastState[this.primaryPath],
                        lwt: lastState._meta.lwt
                    };
                    this.changes$.next(categorized.eventBulk);
                }

                return ret;
            } finally {
                this.openWriteCount$.next(this.openWriteCount$.getValue() - 1);
            }
        });
    }

    async query(
        preparedQuery: ExtendedPreparedQuery<RxDocType>
    ): Promise<RxStorageQueryResult<RxDocType>> {
        return this.admit(() => {
            const plan = this.compileQueryPlan(preparedQuery);
            return this.internals.connectionLease.operation(operations => this.executeQueryPlan(operations, plan));
        });
    }

    private compileQueryPlan(
        preparedQuery: PreparedQuery<RxDocType>
    ): SQLiteJSONQueryPlan<RxDocType> {
        const converter = createMongoQuerySQLConverter({
            regexSupport: this.settings.regexSupport || false,
            query: preparedQuery,
            tableName: this.tableName,
            primaryPath: this.primaryPath as string,
            arrayFields: this.arrayFields
        });
        const sqlQuery = converter.mangoQueryToSQLiteJSONQuery();
        const query = this.settings.queryModifier
            ? this.settings.queryModifier(sqlQuery, preparedQuery)
            : sqlQuery;
        const plan: SQLiteJSONQueryPlan<RxDocType> = { query };

        if (converter.hasUnSpoortedOperators) {
            plan.fallback = {
                matches: getQueryMatcher(this.schema, preparedQuery.query),
                compare: getSortComparator(this.schema, preparedQuery.query),
                skip: preparedQuery.query.skip || 0,
                limit: typeof preparedQuery.query.limit === 'number'
                    ? preparedQuery.query.limit
                    : Infinity
            };
        }
        return plan;
    }

    private async executeQueryPlan(
        operations: SQLiteJSONOperations,
        plan: SQLiteJSONQueryPlan<RxDocType>
    ): Promise<RxStorageQueryResult<RxDocType>> {
        const result = await this.all(operations, plan.query);
        const documents: RxDocumentData<RxDocType>[] = result.map(row => JSON.parse(getDataFromResultRow(row)));
        if (!plan.fallback) {
            return { documents };
        }
        return {
            documents: documents
                .filter(plan.fallback.matches)
                .sort(plan.fallback.compare)
                .slice(plan.fallback.skip, plan.fallback.skip + plan.fallback.limit)
        };
    }

    /**
     * 计数查询
     */
    async count(
        preparedQuery: ExtendedPreparedQuery<RxDocType>
    ): Promise<RxStorageCountResult> {
        return this.admit(() => {
            const plan = this.compileQueryPlan(preparedQuery);
            return this.internals.connectionLease.operation(async operations => {
                if (plan.fallback) {
                    const result = await this.executeQueryPlan(operations, plan);
                    return {
                        count: result.documents.length,
                        mode: 'slow'
                    };
                }

                const countQuery: SQLiteQueryWithParams = {
                    query: `SELECT COUNT(*) AS count FROM (${plan.query.query.trim().replace(/;$/, '')})`,
                    params: plan.query.params,
                    context: plan.query.context
                };
                const result = await this.all(operations, countQuery);
                const countRow = result[0];
                return {
                    count: Array.isArray(countRow) ? countRow[0] : countRow.count,
                    mode: 'fast'
                };
            });
        });
    }

    /**
     * 根据ID查找文档
     */
    async findDocumentsById(
        ids: string[],
        withDeleted: boolean
    ): Promise<RxDocumentData<RxDocType>[]> {
        return this.admit(() => this.internals.connectionLease.operation(
            operations => this.findDocumentsByIdWithOperations(operations, ids, withDeleted)
        ));

    }

    /**
     * 变更流
     */
    changeStream(): Observable<EventBulk<RxStorageChangeEvent<RxDocumentData<RxDocType>>, RxStorageDefaultCheckpoint>> {
        return this.changes$.asObservable();
    }

    /**
     * 获取原生SQLite数据库实例，用于执行原生SQL查询
     */
    async getSQLiteDatabase(): Promise<any> {
        return this.internals.databasePromise;
    }

    /**
     * 清理已删除的文档
     */
    async cleanup(minimumDeletedTime: number): Promise<boolean> {
        return this.admit(async () => {
            const minTimestamp = new Date().getTime() - minimumDeletedTime;
            await this.internals.connectionLease.transaction(async operations => {
                const docsToDelete = await this.all(operations, {
                    query: `SELECT id FROM "${this.tableName}" WHERE deleted = 1 AND lastWriteTime < ?`,
                    params: [minTimestamp],
                    context: { method: 'cleanup_select', data: minimumDeletedTime }
                });
                for (const row of docsToDelete) {
                    const docId = (row as any).id || (Array.isArray(row) ? row[0] : null);
                    if (docId) {
                        for (const fieldPath of this.arrayFields) {
                            await this.run(operations, getMultiKeyIndexDeleteSQL(this.tableName, fieldPath, docId));
                        }
                    }
                }
                await this.run(operations, {
                    query: `DELETE FROM "${this.tableName}" WHERE deleted = 1 AND lastWriteTime < ?`,
                    params: [minTimestamp],
                    context: { method: 'cleanup', data: minimumDeletedTime }
                });
            });
            return true;
        });
    }

    /**
     * 获取附件数据
     * 当前实现不支持附件
     */
    async getAttachmentData(_documentId: string, _attachmentId: string, _digest: string): Promise<Blob> {
        throw newRxError('SNJ1' as any, {
            args: {
                documentId: _documentId,
                attachmentId: _attachmentId
            }
        });
    }

    /**
     * 删除集合
     */
    async remove(): Promise<void> {
        if (this.closed) {
            throw newRxError('SNH', { args: { reason: 'SQLite JSON storage instance is closed' } });
        }
        this.closed = (async () => {
            await this.drainAdmittedOperations();
            try {
                await this.internals.connectionLease.release(async operations => {
                    await this.run(operations, { query: `DROP TABLE IF EXISTS "${this.tableName}"`, params: [], context: { method: 'remove', data: this.tableName } });
                    for (const fieldPath of this.arrayFields) {
                        await this.run(operations, dropMultiKeyIndexTableSQL(this.tableName, fieldPath));
                    }
                }, {
                    databaseName: this.databaseName,
                    collectionName: this.collectionName
                });
            } finally {
                this.changes$.complete();
            }
        })();
        return this.closed;
    }

    /**
     * 关闭存储实例
     */
    async close(): Promise<void> {
        if (!this.closed) {
            this.closed = (async () => {
                await this.drainAdmittedOperations();
                try {
                    await this.internals.connectionLease.release();
                } finally {
                    this.changes$.complete();
                }
            })();
        }
        return this.closed;
    }
}

/**
 * 从 schema 中提取数组类型的字段路径
 * 独立函数，用于在实例创建前提取数组字段
 */
function extractArrayFieldsFromSchema<RxDocType>(
    schema: Readonly<RxJsonSchema<RxDocumentData<RxDocType>>>
): Set<string> {
    const arrayFields = new Set<string>();
    const properties = schema.properties || {};

    const isArrayType = (type: any): boolean => {
        if (type === 'array') return true;
        if (Array.isArray(type)) return type.includes('array');
        return false;
    };

    const traverse = (obj: Record<string, any>, prefix: string) => {
        for (const [key, value] of Object.entries(obj)) {
            const path = prefix ? `${prefix}.${key}` : key;
            if (isArrayType(value?.type)) {
                arrayFields.add(path);
            }
            if (value?.properties) {
                traverse(value.properties, path);
            }
        }
    };

    traverse(properties, '');
    return arrayFields;
}

/**
 * 创建SQLite JSON存储实例
 */
export async function createSQLiteJSONStorageInstance<RxDocType>(
    storage: RxStorageSQLiteJSON,
    params: RxStorageInstanceCreationParams<RxDocType, SQLiteJSONInstanceCreationOptions>,
    settings: SQLiteJSONStorageSettings
): Promise<RxStorageInstanceSQLiteJSON<RxDocType>> {
    const sqliteBasics = settings.sqliteBasics;
    const tableName = params.collectionName + '-' + params.schema.version;

    // 不支持附件
    if (params.schema.attachments) {
        throw newRxError('SNJ1' as any, {
            args: {
                message: 'SQLite JSON storage does not support attachments'
            }
        });
    }

    const useDatabaseName = (settings.databaseNamePrefix ? settings.databaseNamePrefix : '') + '_' + params.databaseName;
    const connectionLease = getSQLiteJSONConnectionLease(sqliteBasics, useDatabaseName);
    const internals: SQLiteJSONInternals = {
        databasePromise: connectionLease.databasePromise,
        connectionLease
    };
    try {
        await connectionLease.transaction(async operations => {
            const tableQuery = `
                CREATE TABLE IF NOT EXISTS "${tableName}"(
                    id TEXT NOT NULL PRIMARY KEY UNIQUE,
                    revision TEXT,
                    deleted BOOLEAN NOT NULL CHECK (deleted IN (0, 1)),
                    lastWriteTime INTEGER NOT NULL,
                    data json
                );`;
            await operations.run({
                query: tableQuery,
                params: [],
                context: { method: 'createSQLiteJSONStorageInstance create tables', data: params.databaseName }
            });
            const indexedFields = params.schema.indexes ?? [];
            for (const field of indexedFields) {
                await operations.run(createJsonIndexSQL(tableName, field as string | string[]));
            }
            const arrayFields = extractArrayFieldsFromSchema(params.schema);
            for (const fieldPath of arrayFields) {
                for (const mkiQuery of createMultiKeyIndexTableSQL(tableName, fieldPath)) {
                    await operations.run(mkiQuery);
                }
            }
        }, () => 'COMMIT', {
            indexCreation: true,
            databaseName: params.databaseName,
            collectionName: params.collectionName
        });
    } catch (err) {
        await connectionLease.release().catch(() => { });
        throw err;
    }

    // 创建存储实例
    const instance = new RxStorageInstanceSQLiteJSON(
        storage,
        params.databaseName,
        params.collectionName,
        params.schema,
        internals as any,
        params.options || {},
        settings,
        tableName,
        params.devMode
    );

    // 添加多实例支持
    addRxStorageMultiInstanceSupport(
        RX_STORAGE_NAME_SQLITE_JSON,
        params,
        instance
    );

    return instance;
}
