import {
    promiseWait,
    errorToPlainJson,
    newRxError
} from '../../index.ts';
import type {
    BulkWriteRow,
    RxDocumentData
} from '../../types/index.d.ts';
import type {
    SQLResultRow,
    SQLiteBasics,
    SQLiteDatabaseClass,
    SQLiteQueryWithParams
} from './sqlite-json-types.ts';

export const RX_STORAGE_NAME_SQLITE_JSON = 'sqlite-json';

/**
 * @link https://www.sqlite.org/inmemorydb.html
 */
export const SQLITE_IN_MEMORY_DB_NAME = ':memory:';

/**
 * 获取数据库连接
 */
export function getDatabaseConnection(
    sqliteBasics: SQLiteBasics<any>,
    databaseName: string
): Promise<SQLiteDatabaseClass> {
    return SQLiteJSONConnectionOwner.forDatabaseName(sqliteBasics, databaseName).acquireDatabase();
}

/**
 * 关闭数据库连接
 */
export function closeDatabaseConnection(
    databaseName: string,
    sqliteBasics: SQLiteBasics<any>
): Promise<void> | void {
    return SQLiteJSONConnectionOwner.forExistingDatabaseName(sqliteBasics, databaseName)?.releaseDatabase();
}

/**
 * 从结果行中获取数据
 */
export function getDataFromResultRow(row: SQLResultRow): string {
    if (!row) {
        return row;
    }

    // 处理数组情况
    if (Array.isArray(row)) {
        return row[4] !== undefined ? row[4] : row[0];
    }

    // 处理对象情况
    if (typeof row === 'object' && row !== null) {
        if ('data' in row) {
            return row.data;
        }
        try {
            const jsonStr = JSON.stringify(row);
            if (jsonStr.startsWith('{') || jsonStr.startsWith('[')) {
                return jsonStr;
            }
        } catch (err) {
            // 如果JSON.stringify失败，继续尝试其他方式
        }
    }

    // 最后尝试转换为字符串
    try {
        const str = String(row);
        if (str.trim().startsWith('{') || str.trim().startsWith('[')) {
            return str;
        }
    } catch (err) {
        console.error('Could not convert row to string:', err);
    }

    // 默认返回空对象
    return '{}';
}

/**
 * 生成SQLite插入SQL
 */
export function getSQLiteJSONInsertSQL<RxDocType>(
    collectionName: string,
    primaryPath: keyof RxDocType,
    docData: RxDocumentData<RxDocType>,
): SQLiteQueryWithParams {
    // language=SQL
    const query = `
        INSERT INTO "${collectionName}" (
            id,
            revision,
            deleted,
            lastWriteTime,
            data
        ) VALUES (
            ?,
            ?,
            ?,
            ?,
            json(?)
        );
    `;
    const params = [
        docData[primaryPath] as string,
        docData._rev,
        docData._deleted ? 1 : 0,
        docData._meta.lwt,
        JSON.stringify(docData)
    ];
    return {
        query,
        params,
        context: {
            method: 'getSQLiteJSONInsertSQL',
            data: {
                collectionName,
                primaryPath
            }
        }
    };
}

/**
 * 生成SQLite更新SQL
 */
export function getSQLiteJSONUpdateSQL<RxDocType>(
    tableName: string,
    primaryPath: keyof RxDocType,
    writeRow: BulkWriteRow<RxDocType>,
): SQLiteQueryWithParams {
    const docData = writeRow.document;
    // language=SQL
    const query = `
    UPDATE "${tableName}" SET
        revision = ?,
        deleted = ?,
        lastWriteTime = ?,
        data = json(?)
        WHERE id = ?;
    `;
    const params = [
        docData._rev,
        docData._deleted ? 1 : 0,
        docData._meta.lwt,
        JSON.stringify(docData),
        docData[primaryPath] as string
    ];
    return {
        query,
        params,
        context: {
            method: 'getSQLiteJSONUpdateSQL',
            data: {
                tableName,
                primaryPath
            }
        }
    };
}

export type SQLiteJSONOperations = {
    run: (queryWithParams: SQLiteQueryWithParams) => Promise<void>;
    all: (queryWithParams: SQLiteQueryWithParams) => Promise<any[]>;
};

// Kept as the public queue view; the owner is its only writer.
export const TX_QUEUE_BY_DATABASE: WeakMap<SQLiteDatabaseClass, Promise<void>> = new WeakMap();

export class SQLiteJSONConnectionOwner {
    private static readonly BY_NAME: Map<string, SQLiteJSONConnectionOwner> = new Map();
    private static readonly BY_DATABASE: WeakMap<SQLiteDatabaseClass, SQLiteJSONConnectionOwner> = new WeakMap();

    static forDatabaseName(
        sqliteBasics: SQLiteBasics<any>,
        databaseName: string
    ): SQLiteJSONConnectionOwner {
        let owner = this.BY_NAME.get(databaseName);
        if (!owner) {
            owner = new SQLiteJSONConnectionOwner(sqliteBasics, sqliteBasics.open(databaseName));
            this.BY_NAME.set(databaseName, owner);
            owner.databasePromise.catch(() => this.removeFromCache(owner!));
        } else if (owner.sqliteBasics !== sqliteBasics && databaseName !== SQLITE_IN_MEMORY_DB_NAME) {
            throw new Error('opened db with different creator method ' + databaseName + ' ' + owner.sqliteBasics.debugId + ' ' + sqliteBasics.debugId);
        }
        return owner;
    }

    static forExistingDatabaseName(
        sqliteBasics: SQLiteBasics<any>,
        databaseName: string
    ): SQLiteJSONConnectionOwner | undefined {
        const owner = this.BY_NAME.get(databaseName);
        if (owner && owner.sqliteBasics !== sqliteBasics && databaseName !== SQLITE_IN_MEMORY_DB_NAME) {
            throw new Error('opened db with different creator method ' + databaseName + ' ' + owner.sqliteBasics.debugId + ' ' + sqliteBasics.debugId);
        }
        return owner;
    }

    static forDatabase(
        database: SQLiteDatabaseClass,
        sqliteBasics: SQLiteBasics<any>
    ): SQLiteJSONConnectionOwner {
        let owner = this.BY_DATABASE.get(database);
        if (!owner) {
            owner = new SQLiteJSONConnectionOwner(sqliteBasics, Promise.resolve(database));
            this.BY_DATABASE.set(database, owner);
        }
        return owner;
    }

    private static removeFromCache(owner: SQLiteJSONConnectionOwner): void {
        for (const [databaseName, cachedOwner] of this.BY_NAME) {
            if (cachedOwner === owner) {
                this.BY_NAME.delete(databaseName);
            }
        }
    }

    private accepting = true;
    private broken?: any;
    private queue: Promise<void> = Promise.resolve();
    private leaseCount = 0;
    private readonly databaseLeases: SQLiteJSONConnectionLease[] = [];
    readonly databasePromise: Promise<SQLiteDatabaseClass>;

    private constructor(
        private readonly sqliteBasics: SQLiteBasics<any>,
        databasePromise: Promise<SQLiteDatabaseClass>
    ) {
        this.databasePromise = databasePromise.then(database => {
            SQLiteJSONConnectionOwner.BY_DATABASE.set(database, this);
            return database;
        });
    }

    acquire(): SQLiteJSONConnectionLease {
        if (!this.accepting) {
            throw newRxError('SNH', { args: { reason: 'SQLite JSON connection is closed' } });
        }
        this.leaseCount++;
        return new SQLiteJSONConnectionLease(this);
    }

    acquireDatabase(): Promise<SQLiteDatabaseClass> {
        const lease = this.acquire();
        this.databaseLeases.push(lease);
        return lease.databasePromise;
    }

    releaseDatabase(): Promise<void> | void {
        return this.databaseLeases.shift()?.release();
    }

    private publishQueue(): void {
        const queue = this.queue;
        this.databasePromise.then(database => {
            if (queue === this.queue) {
                TX_QUEUE_BY_DATABASE.set(database, queue);
            }
        }).catch(() => { });
    }

    private enqueue<T>(operation: (database: SQLiteDatabaseClass) => Promise<T>): Promise<T> {
        if (!this.accepting) {
            return Promise.reject(newRxError('SNH', { args: { reason: 'SQLite JSON connection is closed' } }));
        }
        if (this.broken) {
            return Promise.reject(this.broken);
        }
        const run = this.queue.then(async () => {
            if (this.broken) {
                throw this.broken;
            }
            return operation(await this.databasePromise);
        });
        this.queue = run.then(() => undefined, () => undefined);
        this.publishQueue();
        return run;
    }

    operation<T>(operation: (operations: SQLiteJSONOperations) => Promise<T>): Promise<T> {
        return this.enqueue(async database => operation({
            run: query => this.sqliteBasics.run(database, query),
            all: query => this.sqliteBasics.all(database, query)
        }));
    }

    transaction<T>(
        operation: (operations: SQLiteJSONOperations) => Promise<T>,
        finishMode: (result: T) => 'COMMIT' | 'ROLLBACK' = () => 'COMMIT',
        context?: any
    ): Promise<T> {
        return this.enqueue(async database => {
            await openSqliteTransaction(database, this.sqliteBasics);
            const operations = {
                run: (query: SQLiteQueryWithParams) => this.sqliteBasics.run(database, query),
                all: (query: SQLiteQueryWithParams) => this.sqliteBasics.all(database, query)
            };
            try {
                const result = await operation(operations);
                await finishSqliteTransaction(database, this.sqliteBasics, finishMode(result), context);
                return result;
            } catch (err) {
                try {
                    await finishSqliteTransaction(database, this.sqliteBasics, 'ROLLBACK', context);
                } catch (rollbackError) {
                    this.broken = rollbackError;
                }
                throw err;
            }
        });
    }

    async release(
        operation?: (operations: SQLiteJSONOperations) => Promise<void>,
        context?: any
    ): Promise<void> {
        let hasOperationError = false;
        let operationError: any;
        if (operation) {
            try {
                await this.transaction(operation, () => 'COMMIT', context);
            } catch (err) {
                hasOperationError = true;
                operationError = err;
            }
        }

        let hasReleaseError = false;
        let releaseError: any;
        try {
            this.leaseCount--;
            if (this.leaseCount === 0) {
                SQLiteJSONConnectionOwner.removeFromCache(this);
                this.accepting = false;
                const close = this.queue.then(async () => {
                    await this.sqliteBasics.close(await this.databasePromise);
                });
                this.queue = close.then(() => undefined, () => undefined);
                this.publishQueue();
                await close;
            }
        } catch (err) {
            hasReleaseError = true;
            releaseError = err;
        }

        if (hasOperationError) {
            throw operationError;
        }
        if (hasReleaseError) {
            throw releaseError;
        }
    }
}

export class SQLiteJSONConnectionLease {
    readonly databasePromise: Promise<SQLiteDatabaseClass>;
    private releasePromise?: Promise<void>;

    constructor(private readonly owner: SQLiteJSONConnectionOwner) {
        this.databasePromise = owner.databasePromise;
    }

    private ensureActive(): void {
        if (this.releasePromise) {
            throw newRxError('SNH', { args: { reason: 'SQLite JSON connection lease is released' } });
        }
    }

    operation<T>(operation: (operations: SQLiteJSONOperations) => Promise<T>): Promise<T> {
        this.ensureActive();
        return this.owner.operation(operation);
    }

    transaction<T>(
        operation: (operations: SQLiteJSONOperations) => Promise<T>,
        finishMode: (result: T) => 'COMMIT' | 'ROLLBACK' = () => 'COMMIT',
        context?: any
    ): Promise<T> {
        this.ensureActive();
        return this.owner.transaction(operation, finishMode, context);
    }

    release(
        operation?: (operations: SQLiteJSONOperations) => Promise<void>,
        context?: any
    ): Promise<void> {
        if (!this.releasePromise) {
            this.releasePromise = this.owner.release(operation, context);
        }
        return this.releasePromise;
    }
}

export function getSQLiteJSONConnectionOwner(
    database: SQLiteDatabaseClass,
    sqliteBasics: SQLiteBasics<any>,
    _databaseName?: string,
    _context?: any
): SQLiteJSONConnectionOwner {
    return SQLiteJSONConnectionOwner.forDatabase(database, sqliteBasics);
}

export function getSQLiteJSONConnectionLease(
    sqliteBasics: SQLiteBasics<any>,
    databaseName: string
): SQLiteJSONConnectionLease {
    return SQLiteJSONConnectionOwner.forDatabaseName(sqliteBasics, databaseName).acquire();
}

/** Legacy transaction entry point, serialized by the same owner as storage operations. */
export function sqliteTransaction(
    database: SQLiteDatabaseClass,
    sqliteBasics: SQLiteBasics<any>,
    handler: () => Promise<'COMMIT' | 'ROLLBACK'>,
    context?: any
): Promise<void> {
    return getSQLiteJSONConnectionOwner(database, sqliteBasics)
        .transaction(handler, mode => mode, context)
        .then(() => undefined);
}

/**
 * 打开SQLite事务
 */
export async function openSqliteTransaction(
    database: SQLiteDatabaseClass,
    sqliteBasics: SQLiteBasics<any>
) {
    let openedTransaction = false;
    while (!openedTransaction) {
        try {
            await sqliteBasics.run(
                database,
                {
                    query: 'BEGIN;',
                    params: [],
                    context: {
                        method: 'openSqliteTransaction',
                        data: ''
                    }
                }
            );
            openedTransaction = true;
        } catch (err: any) {
            console.log('open transaction error (will retry):');
            const errorAsJson = errorToPlainJson(err);
            console.log(errorAsJson);
            console.dir(err);
            if (
                err.message && (
                    err.message.includes('Database is closed') ||
                    err.message.includes('API misuse')
                )
            ) {
                throw err;
            }
            // 等待一个tick，避免在错误时完全阻塞CPU
            await promiseWait(0);
        }
    }
    return;
}

/**
 * 完成SQLite事务
 */
export async function finishSqliteTransaction(
    database: SQLiteDatabaseClass,
    sqliteBasics: SQLiteBasics<any>,
    mode: 'COMMIT' | 'ROLLBACK',
    /**
     * 上下文信息，用于调试
     */
    context?: any
) {
    return sqliteBasics.run(
        database,
        {
            query: mode + ';',
            params: [],
            context: {
                method: 'finishSqliteTransaction',
                data: mode
            }
        }
    ).catch(err => {
        if (context) {
            console.error('cannot close transaction (mode: ' + mode + ')');
            console.log(JSON.stringify(context, null, 4));
        }
        throw err;
    });
}

/**
 * 参数占位符
 */
export const PARAM_KEY = '?';

/**
 * 确保参数数量正确
 */
export function ensureParamsCountIsCorrect(queryWithParams: SQLiteQueryWithParams) {
    const paramsCount = queryWithParams.params.length;
    const paramKeyCount = queryWithParams.query.split(PARAM_KEY).length - 1;
    if (paramsCount !== paramKeyCount) {
        throw new Error('ensureParamsCountIsCorrect() wrong param count: ' + JSON.stringify(queryWithParams));
    }
}

/**
 * 将布尔参数转换为整数
 * SQLite不支持布尔类型，使用整数代替
 * @link https://stackoverflow.com/a/2452569/3443137
 */
export function boolParamsToInt(params: any[]): any[] {
    return params.map(p => {
        if (typeof p === 'boolean') {
            if (p) {
                return 1;
            } else {
                return 0;
            }
        } else {
            return p;
        }
    });
}

/**
 * 生成JSON路径表达式
 * 将JavaScript路径转换为SQLite JSON路径
 * 例如：'user.name' -> '$.user.name'
 */
export function generateJsonPathExpression(path: string): string {
    if (path.startsWith('$')) {
        return path;
    }
    return '$.' + path;
}

/**
 * 创建JSON索引SQL
 * 支持单字段索引和联合索引
 */
export function createJsonIndexSQL(
    tableName: string,
    fieldPath: string | string[],
    indexName?: string
): SQLiteQueryWithParams {
    // 处理字段路径，可以是单个字段或字段数组（联合索引）
    const isArray = Array.isArray(fieldPath);
    const fieldPaths = isArray ? fieldPath : [fieldPath];

    // 生成索引名称
    const fieldNamePart = isArray
        ? fieldPaths.map(f => f.replace(/\./g, '_')).join('_')
        : fieldPaths[0]?.replace(/\./g, '_') || '';
    const actualIndexName = indexName || `${tableName}_${fieldNamePart}_idx`;

    // 生成索引字段列表
    const indexColumns = fieldPaths.map(field => {
        const jsonPath = generateJsonPathExpression(field);
        return `json_extract(data, '${jsonPath}')`;
    }).join(', ');

    // language=SQL
    const query = `
        CREATE INDEX IF NOT EXISTS "${actualIndexName}"
        ON "${tableName}" (${indexColumns});
    `;

    return {
        query,
        params: [],
        context: {
            method: 'createJsonIndexSQL',
            data: {
                tableName,
                fieldPath
            }
        }
    };
}

/**
 * 获取多键索引表名
 * 命名格式: {主表名}_mki_{字段路径(点号替换为下划线)}
 */
export function getMultiKeyIndexTableName(tableName: string, fieldPath: string): string {
    const sanitizedFieldPath = fieldPath.replace(/\./g, '_');
    return `${tableName}_mki_${sanitizedFieldPath}`;
}

/**
 * 创建多键索引表的SQL
 * 用于存储数组字段中的各个元素，实现高效的数组包含查询
 */
export function createMultiKeyIndexTableSQL(
    tableName: string,
    fieldPath: string
): SQLiteQueryWithParams[] {
    const mkiTableName = getMultiKeyIndexTableName(tableName, fieldPath);

    const queries: SQLiteQueryWithParams[] = [];

    // 创建多键索引表，value 使用 JSON 类型保留原始类型信息
    queries.push({
        query: `
            CREATE TABLE IF NOT EXISTS "${mkiTableName}" (
                doc_id TEXT NOT NULL,
                value JSON NOT NULL,
                PRIMARY KEY (doc_id, value)
            );
        `,
        params: [],
        context: {
            method: 'createMultiKeyIndexTableSQL',
            data: { tableName, fieldPath }
        }
    });

    // 创建值索引，用于快速查找包含特定值的文档
    queries.push({
        query: `
            CREATE INDEX IF NOT EXISTS "${mkiTableName}_val_idx"
            ON "${mkiTableName}" (value);
        `,
        params: [],
        context: {
            method: 'createMultiKeyIndexTableSQL_value_index',
            data: { tableName, fieldPath }
        }
    });

    return queries;
}

/**
 * 生成插入多键索引条目的SQL
 * 将文档中数组字段的每个元素插入到索引表中
 */
export function getMultiKeyIndexInsertSQL(
    tableName: string,
    fieldPath: string,
    docId: string,
    arrayValues: any[]
): SQLiteQueryWithParams[] {
    const mkiTableName = getMultiKeyIndexTableName(tableName, fieldPath);
    const queries: SQLiteQueryWithParams[] = [];

    for (const value of arrayValues) {
        // 只跳过 undefined，允许 null 值插入多键索引表
        // null 会被 JSON.stringify 序列化为 "null"
        if (value !== undefined) {
            queries.push({
                query: `INSERT OR IGNORE INTO "${mkiTableName}" (doc_id, value) VALUES (?, json(?));`,
                params: [docId, JSON.stringify(value)],
                context: {
                    method: 'getMultiKeyIndexInsertSQL',
                    data: { tableName, fieldPath, docId }
                }
            });
        }
    }

    return queries;
}

/**
 * 生成删除文档多键索引条目的SQL
 * 用于在更新或删除文档时清理旧的索引条目
 */
export function getMultiKeyIndexDeleteSQL(
    tableName: string,
    fieldPath: string,
    docId: string
): SQLiteQueryWithParams {
    const mkiTableName = getMultiKeyIndexTableName(tableName, fieldPath);

    return {
        query: `DELETE FROM "${mkiTableName}" WHERE doc_id = ?;`,
        params: [docId],
        context: {
            method: 'getMultiKeyIndexDeleteSQL',
            data: { tableName, fieldPath, docId }
        }
    };
}

/**
 * 从嵌套对象中获取字段值
 * 支持点号分隔的路径，如 'user.tags'
 */
export function getNestedValue(obj: any, path: string): any {
    const parts = path.split('.');
    let current = obj;

    for (const part of parts) {
        if (current === null || current === undefined) {
            return undefined;
        }
        current = current[part];
    }

    return current;
}

/**
 * 删除多键索引表的SQL
 * 用于在删除集合时清理索引表
 */
export function dropMultiKeyIndexTableSQL(
    tableName: string,
    fieldPath: string
): SQLiteQueryWithParams {
    const mkiTableName = getMultiKeyIndexTableName(tableName, fieldPath);

    return {
        query: `DROP TABLE IF EXISTS "${mkiTableName}";`,
        params: [],
        context: {
            method: 'dropMultiKeyIndexTableSQL',
            data: { tableName, fieldPath }
        }
    };
}
