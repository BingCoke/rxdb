import {
    getPrimaryFieldOfPrimaryKey,
    categorizeBulkWriteRows,
    ensureNotFalsy,
    addRxStorageMultiInstanceSupport,
    promiseWait,
    getQueryMatcher,
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
    PreparedQuery,
    RxStorage
} from '../../types/index.d.ts';
import { BehaviorSubject, Observable, Subject, filter, firstValueFrom } from 'rxjs';
import {
    closeDatabaseConnection,
    ensureParamsCountIsCorrect,
    getDatabaseConnection,
    getSQLiteJSONUpdateSQL,
    RX_STORAGE_NAME_SQLITE_JSON,
    sqliteTransaction,
    getDataFromResultRow,
    getSQLiteJSONInsertSQL,
    TX_QUEUE_BY_DATABASE,
    createJsonIndexSQL,
    mangoQueryToSQLiteJSON
} from './sqlite-json-helpers.ts';
import type {
    SQLiteJSONInstanceCreationOptions,
    SQLiteJSONInternals,
    SQLiteQueryWithParams,
    SQLiteJSONStorageSettings,
    ExtendedPreparedQuery
    , SQLiteBasics
} from './sqlite-json-types.ts';
export * from './sqlite-json-helpers.ts';
export * from './sqlite-json-types.ts';



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
    }

    /**
     * 执行SQL查询，不返回结果
     */
    run(
        db: any,
        queryWithParams: SQLiteQueryWithParams
    ): Promise<void> {
        if (this.devMode) {
            ensureParamsCountIsCorrect(queryWithParams);
        }
        return this.sqliteBasics.run(db, queryWithParams);
    }

    /**
     * 执行SQL查询，返回结果
     */
    all(
        db: any,
        queryWithParams: SQLiteQueryWithParams
    ): Promise<any[]> {
        if (this.devMode) {
            ensureParamsCountIsCorrect(queryWithParams);
        }
        return this.sqliteBasics.all(db, queryWithParams);
    }

    /**
     * 批量写入文档
     */
    async bulkWrite(
        documentWrites: BulkWriteRow<RxDocType>[],
        context: string
    ): Promise<RxStorageBulkWriteResponse<RxDocType>> {
        this.openWriteCount$.next(this.openWriteCount$.getValue() + 1);
        const database = await this.internals.databasePromise;
        const ret: RxStorageBulkWriteResponse<RxDocType> = {
            error: []
        };
        const writePromises: Promise<any>[] = [];
        let categorized: CategorizeBulkWriteRowsOutput<RxDocType> = {} as any;

        await sqliteTransaction(
            database,
            this.sqliteBasics,
            async () => {
                if (this.closed) {
                    this.openWriteCount$.next(this.openWriteCount$.getValue() - 1);
                    throw new Error('SQLiteJSON.bulkWrite(' + context + ') already closed ' + this.tableName + ' context: ' + context);
                }

                // 只查询需要操作的文档ID
                const docIds = documentWrites.map(d => (d.document as any)[this.primaryPath]);

                const result = await this.findDocumentsById(docIds, true)
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
                categorized.bulkInsertDocs.forEach(row => {
                    const insertQuery = getSQLiteJSONInsertSQL(
                        this.tableName,
                        this.primaryPath as any,
                        row.document,
                    );
                    writePromises.push(
                        this.run(
                            database,
                            insertQuery
                        )
                    );
                });

                // 执行更新操作
                categorized.bulkUpdateDocs.forEach(row => {
                    const updateQuery = getSQLiteJSONUpdateSQL<RxDocType>(
                        this.tableName,
                        this.primaryPath,
                        row,
                    );
                    writePromises.push(
                        this.run(
                            database,
                            updateQuery
                        )
                    );
                });

                await Promise.all(writePromises);

                // 关闭事务
                if (this.closed) {
                    this.openWriteCount$.next(this.openWriteCount$.getValue() - 1);
                    return 'ROLLBACK';
                } else {
                    this.openWriteCount$.next(this.openWriteCount$.getValue() - 1);
                    return 'COMMIT';
                }
            },
            {
                databaseName: this.databaseName,
                collectionName: this.collectionName
            }
        );

        // 发送变更事件
        if (categorized && categorized.eventBulk.events.length > 0) {
            const lastState = ensureNotFalsy(categorized.newestRow).document;
            categorized.eventBulk.checkpoint = {
                id: lastState[this.primaryPath],
                lwt: lastState._meta.lwt
            };
            this.changes$.next(categorized.eventBulk);
        }

        return ret;
    }

    /**
     * 将Mango查询转换为SQLite JSON查询
     * 利用SQLite的JSON函数高效查询嵌套数据
     */
    private mangoQueryToSQLiteJSONQuery<RxDocType>(
        query: PreparedQuery<RxDocType>
    ): SQLiteQueryWithParams {
        const mangoQuery = query.query;
        const selector = mangoQuery.selector || {};

        // 构建WHERE子句
        const whereClauses: string[] = [];
        const params: any[] = [];
        const nonImplementedOperators: string[] = [];

        // 处理选择器中的每个字段
        Object.entries(selector).forEach(([field, condition]) => {
            if (field === '_id') {
                field = this.primaryPath as string;
            }

            // 如果条件是一个简单值，将其视为$eq操作符
            if (typeof condition !== 'object' || condition === null) {
                const { sql, params: fieldParams } = mangoQueryToSQLiteJSON(field, '$eq', condition);
                whereClauses.push(sql);
                params.push(...fieldParams);
            } else {
                // 处理空对象作为查询条件的特殊情况
                if (typeof condition === 'object' && condition !== null && Object.keys(condition).length === 0) {
                    // 空对象作为查询条件，应该返回一个始终为假的条件
                    // 使用 1=0 确保条件永远为假，不会匹配任何文档
                    whereClauses.push('1=0');
                } else {
                    // 处理复杂条件（包含操作符的对象）
                    Object.entries(condition as Record<string, any>).forEach(([operator, value]) => {
                        // 检查是否为不支持的操作符
                        if (operator === '$regex' || operator === '$options') {
                            if (!nonImplementedOperators.includes(operator)) {
                                nonImplementedOperators.push(operator);
                            }
                        }

                        const { sql, params: opParams } = mangoQueryToSQLiteJSON(field, operator, value);
                        whereClauses.push(sql);
                        params.push(...opParams);
                    });
                }
            }
        });

        // 处理$or操作符
        if (selector.$or && Array.isArray(selector.$or) && selector.$or.length > 0) {
            const orClauses: string[] = [];
            const orParams: any[] = [];

            selector.$or.forEach(orCondition => {
                const orWhereClauses: string[] = [];

                Object.entries(orCondition).forEach(([field, condition]) => {
                    if (field === '_id') {
                        field = this.primaryPath as string;
                    }

                    if (typeof condition !== 'object' || condition === null) {
                        const { sql, params: fieldParams } = mangoQueryToSQLiteJSON(field, '$eq', condition);
                        orWhereClauses.push(sql);
                        orParams.push(...fieldParams);
                    } else {
                        Object.entries(condition as Record<string, any>).forEach(([operator, value]) => {
                            // 检查是否为不支持的操作符
                            if (operator === '$regex' || operator === '$options') {
                                if (!nonImplementedOperators.includes(operator)) {
                                    nonImplementedOperators.push(operator);
                                }
                            }

                            const { sql, params: opParams } = mangoQueryToSQLiteJSON(field, operator, value);
                            orWhereClauses.push(sql);
                            orParams.push(...opParams);
                        });
                    }
                });

                if (orWhereClauses.length > 0) {
                    orClauses.push(`(${orWhereClauses.join(' AND ')})`);
                }
            });

            if (orClauses.length > 0) {
                whereClauses.push(`(${orClauses.join(' OR ')})`);
                params.push(...orParams);
            }
        }

        // 处理$and操作符
        if (selector.$and && Array.isArray(selector.$and) && selector.$and.length > 0) {
            const andClauses: string[] = [];
            const andParams: any[] = [];

            selector.$and.forEach(andCondition => {
                const andWhereClauses: string[] = [];

                Object.entries(andCondition).forEach(([field, condition]) => {
                    if (field === '_id') {
                        field = this.primaryPath as string;
                    }

                    if (typeof condition !== 'object' || condition === null) {
                        const { sql, params: fieldParams } = mangoQueryToSQLiteJSON(field, '$eq', condition);
                        andWhereClauses.push(sql);
                        andParams.push(...fieldParams);
                    } else {
                        Object.entries(condition as Record<string, any>).forEach(([operator, value]) => {
                            // 检查是否为不支持的操作符
                            if (operator === '$regex' || operator === '$options') {
                                if (!nonImplementedOperators.includes(operator)) {
                                    nonImplementedOperators.push(operator);
                                }
                            }

                            const { sql, params: opParams } = mangoQueryToSQLiteJSON(field, operator, value);
                            andWhereClauses.push(sql);
                            andParams.push(...opParams);
                        });
                    }
                });

                if (andWhereClauses.length > 0) {
                    andClauses.push(`(${andWhereClauses.join(' AND ')})`);
                }
            });

            if (andClauses.length > 0) {
                whereClauses.push(`(${andClauses.join(' AND ')})`);
                params.push(...andParams);
            }
        }

        // 构建ORDER BY子句
        let orderByClause = '';
        if (mangoQuery.sort) {
            const sortParts = mangoQuery.sort.map(sortObj => {
                const field = Object.keys(sortObj)[0];
                if (!field) {
                    return '';
                }

                const direction = sortObj[field] === 'desc' ? 'DESC' : 'ASC';
                let jsonPath;

                if (field === '_id') {
                    jsonPath = 'id';
                } else if (field.includes('.')) {
                    // 处理嵌套字段
                    jsonPath = `json_extract(data, '$.${field}')`;
                } else {
                    jsonPath = `json_extract(data, '$.${field}')`;
                }

                return `${jsonPath} ${direction}`;
            });

            if (sortParts.length > 0) {
                orderByClause = `ORDER BY ${sortParts.join(', ')}`;
            }
        }

        // 构建LIMIT和SKIP子句
        let limitSkipClause = '';
        if (mangoQuery.limit) {
            limitSkipClause = `LIMIT ${mangoQuery.limit}`;
            if (mangoQuery.skip) {
                limitSkipClause += ` OFFSET ${mangoQuery.skip}`;
            }
        } else if (mangoQuery.skip) {
            limitSkipClause = `LIMIT -1 OFFSET ${mangoQuery.skip}`;
        }

        // 组合完整的SQL查询
        const whereClause = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';
        const query_sql = `SELECT id, data FROM "${this.tableName}" ${whereClause} ${orderByClause} ${limitSkipClause}`;

        // 如果有不支持的操作符，将其添加到查询对象中
        if (nonImplementedOperators.length > 0) {
            (query as ExtendedPreparedQuery<RxDocType>).nonImplementedOperators = nonImplementedOperators;
        }

        return {
            query: query_sql,
            params,
            context: {
                method: 'query',
                data: query
            }
        };
    }

    /**
     * 查询文档
     */
    /**
     * 打印查询信息(包含EXPLAIN结果)
     */
    private async logQueryInfo(
        sqlQuery: SQLiteQueryWithParams,
        preparedQuery: ExtendedPreparedQuery<RxDocType>
    ) {
        try {
            const database = await this.internals.databasePromise;
            const explainQuery = {
                query: 'EXPLAIN QUERY PLAN ' + sqlQuery.query,
                params: sqlQuery.params,
                context: sqlQuery.context
            };

            const explainResult = await this.all(database, explainQuery);

            let output = `\nSQLite Query Plan for table ${this.tableName}:\n`;
            output += `SQL: ${sqlQuery.query}\n`;
            output += `Params: ${JSON.stringify(sqlQuery.params)}\n`;
            output += 'EXPLAIN RESULT:\n';
            explainResult.forEach(row => {
                output += `${row.detail}\n`;
            });
            output += `Non-implemented Operators: ${JSON.stringify(preparedQuery.nonImplementedOperators || [])}\n`;

            console.log(output);
        } catch (err) {
            console.error('Failed to explain query:', err);
        }
    }

    async query(
        preparedQuery: ExtendedPreparedQuery<RxDocType>
    ): Promise<RxStorageQueryResult<RxDocType>> {
        const database = await this.internals.databasePromise;

        // 将Mango查询转换为SQLite JSON查询
        const sqlQuery = this.mangoQueryToSQLiteJSONQuery(preparedQuery);

        console.log("search query is " + JSON.stringify(preparedQuery, null, 2));
        // 打印查询信息
        this.logQueryInfo(sqlQuery, preparedQuery);

        // 应用查询修改器（如果有）
        const finalQuery = this.settings.queryModifier
            ? this.settings.queryModifier(sqlQuery, preparedQuery as any)
            : sqlQuery;

        // 执行查询
        const result = await this.all(database, finalQuery);

        // 解析结果
        const documents: RxDocumentData<RxDocType>[] = result.map(row => {
            return JSON.parse(getDataFromResultRow(row));
        });

        // 检查是否有不支持的操作符
        if (preparedQuery.nonImplementedOperators && preparedQuery.nonImplementedOperators.length > 0) {
            // 过滤掉已实现的操作符
            const notImplemented = preparedQuery.nonImplementedOperators;

            // 如果还有其他未实现的操作符，则在内存中进行过滤
            if (notImplemented.length > 0) {
                const queryMatcher = getQueryMatcher(this.schema, preparedQuery.query);
                return {
                    documents: documents.filter(doc => queryMatcher(doc))
                };
            }
        }

        return {
            documents
        };
    }

    /**
     * 计数查询
     */
    async count(
        preparedQuery: ExtendedPreparedQuery<RxDocType>
    ): Promise<RxStorageCountResult> {
        const database = await this.internals.databasePromise;

        // 如果有不支持的操作符，使用内存中过滤
        if (preparedQuery.nonImplementedOperators && preparedQuery.nonImplementedOperators.length > 0) {
            // 过滤掉已实现的操作符
            const notImplemented = preparedQuery.nonImplementedOperators;

            // 如果还有其他未实现的操作符，则在内存中进行过滤
            if (notImplemented.length > 0) {
                const results = await this.query(preparedQuery);
                return {
                    count: results.documents.length,
                    mode: 'slow'
                };
            }
        }

        // 将Mango查询转换为SQLite JSON查询
        const sqlQuery = this.mangoQueryToSQLiteJSONQuery(preparedQuery);

        // 修改查询以使用COUNT
        const countQuery = sqlQuery.query.replace(
            /SELECT id, data FROM/i,
            'SELECT COUNT(*) as count FROM'
        );

        // 移除ORDER BY子句（对COUNT没有影响）
        const queryWithoutOrder = countQuery.replace(/ORDER BY.*?(LIMIT|$)/i, '$1');

        // 执行查询
        const result = await this.all(
            database,
            {
                query: queryWithoutOrder,
                params: sqlQuery.params,
                context: sqlQuery.context
            }
        );

        return {
            count: result[0].count,
            mode: 'fast'
        };
    }

    /**
     * 根据ID查找文档
     */
    async findDocumentsById(
        ids: string[],
        withDeleted: boolean
    ): Promise<RxDocumentData<RxDocType>[]> {
        const database = await this.internals.databasePromise;

        if (this.closed) {
            throw new Error('SQLiteJSON.findDocumentsById() already closed ' + this.tableName);
        }

        if (ids.length === 0) {
            return [];
        }

        // 构建查询
        const placeholders = ids.map(() => '?').join(', ');
        let query = `SELECT data FROM "${this.tableName}" WHERE id IN (${placeholders})`;

        if (!withDeleted) {
            query += ` AND deleted = 0`;
        }
        const result = await this.all(
            database,
            {
                query,
                params: ids,
                context: {
                    method: 'findDocumentsById',
                    data: ids
                }
            }
        );

        // 解析结果
        return result.map(row => {
            try {
                const rowData = getDataFromResultRow(row);
                if (typeof rowData === 'string') {
                    return JSON.parse(rowData);
                } else if (typeof rowData === 'object' && rowData !== null) {
                    return rowData;
                }
                return {};
            } catch (err) {
                console.error('Failed to parse document data:', err);
                return {};
            }
        });
    }

    /**
     * 变更流
     */
    changeStream(): Observable<EventBulk<RxStorageChangeEvent<RxDocumentData<RxDocType>>, RxStorageDefaultCheckpoint>> {
        return this.changes$.asObservable();
    }

    /**
     * 清理已删除的文档
     */
    async cleanup(minimumDeletedTime: number): Promise<boolean> {
        await promiseWait(0);
        const database = await this.internals.databasePromise;

        // 清理已删除的文档
        const minTimestamp = new Date().getTime() - minimumDeletedTime;
        await this.run(
            database,
            {
                query: `
                    DELETE FROM
                        "${this.tableName}"
                    WHERE
                        deleted = 1
                        AND
                        lastWriteTime < ?
                `,
                params: [
                    minTimestamp
                ],
                context: {
                    method: 'cleanup',
                    data: minimumDeletedTime
                }
            }
        );
        return true;
    }

    /**
     * 获取附件数据
     * 当前实现不支持附件
     */
    async getAttachmentData(_documentId: string, _attachmentId: string): Promise<string> {
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
            throw new Error('closed already');
        }
        const database = await this.internals.databasePromise;
        const promises = [
            this.run(
                database,
                {
                    query: `DROP TABLE IF EXISTS "${this.tableName}"`,
                    params: [],
                    context: {
                        method: 'remove',
                        data: this.tableName
                    }
                }
            )
        ];
        await Promise.all(promises);
        return this.close();
    }

    /**
     * 关闭存储实例
     */
    async close(): Promise<void> {
        const queue = TX_QUEUE_BY_DATABASE.get(await this.internals.databasePromise);
        if (queue) {
            await queue;
        }

        if (this.closed) {
            return this.closed;
        }
        this.closed = (async () => {
            await firstValueFrom(this.openWriteCount$.pipe(filter(v => v === 0)));
            const database = await this.internals.databasePromise;

            // 首先获取事务，确保当前运行的操作已完成
            await sqliteTransaction(
                database,
                this.sqliteBasics,
                () => {
                    return Promise.resolve('COMMIT');
                }
            ).catch(() => { });
            this.changes$.complete();
            await closeDatabaseConnection(
                this.databaseName,
                this.storage.settings.sqliteBasics
            );
        })();
        return this.closed;
    }
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

    const internals: Partial<SQLiteJSONInternals> = {};
    const useDatabaseName = (settings.databaseNamePrefix ? settings.databaseNamePrefix : '') + '_' + params.databaseName;

    // 获取数据库连接
    internals.databasePromise = getDatabaseConnection(
        storage.settings.sqliteBasics,
        useDatabaseName
    ).then(async (database) => {
        await sqliteTransaction(
            database,
            sqliteBasics,
            async () => {
                // 创建表
                const tableQuery = `
                CREATE TABLE IF NOT EXISTS "${tableName}"(
                    id TEXT NOT NULL PRIMARY KEY UNIQUE,
                    revision TEXT,
                    deleted BOOLEAN NOT NULL CHECK (deleted IN (0, 1)),
                    lastWriteTime INTEGER NOT NULL,
                    data json
                );
                `;
                await sqliteBasics.run(
                    database,
                    {
                        query: tableQuery,
                        params: [],
                        context: {
                            method: 'createSQLiteJSONStorageInstance create tables',
                            data: params.databaseName
                        }
                    }
                );

                // 确定要索引的字段
                const indexedFields = params.schema.indexes ?? []

                for (const field of indexedFields) {
                    const indexQuery = createJsonIndexSQL(tableName, field as string | string[]);
                    await sqliteBasics.run(
                        database,
                        indexQuery
                    );
                }

                return 'COMMIT';
            },
            {
                indexCreation: true,
                databaseName: params.databaseName,
                collectionName: params.collectionName
            }
        );
        return database;
    });

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
    await addRxStorageMultiInstanceSupport(
        RX_STORAGE_NAME_SQLITE_JSON,
        params,
        instance
    );

    return instance;
}
