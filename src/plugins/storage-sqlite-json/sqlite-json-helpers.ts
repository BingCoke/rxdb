import {
    PROMISE_RESOLVE_VOID,
    promiseWait,
    errorToPlainJson
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
 * 数据库状态
 */
type DatabaseState = {
    database: Promise<SQLiteDatabaseClass>;
    openConnections: number;
    sqliteBasics: SQLiteBasics<SQLiteDatabaseClass>;
}

/**
 * 按名称存储数据库状态
 */
const DATABASE_STATE_BY_NAME: Map<string, DatabaseState> = new Map();

/**
 * 获取数据库连接
 */
export function getDatabaseConnection(
    sqliteBasics: SQLiteBasics<any>,
    databaseName: string
): Promise<SQLiteDatabaseClass> {
    let state = DATABASE_STATE_BY_NAME.get(databaseName);
    if (!state) {
        state = {
            database: sqliteBasics.open(databaseName),
            sqliteBasics,
            openConnections: 1
        };
        DATABASE_STATE_BY_NAME.set(databaseName, state);
    } else {
        if (state.sqliteBasics !== sqliteBasics && databaseName !== SQLITE_IN_MEMORY_DB_NAME) {
            throw new Error('opened db with different creator method ' + databaseName + ' ' + state.sqliteBasics.debugId + ' ' + sqliteBasics.debugId);
        }
        state.openConnections = state.openConnections + 1;
    }
    return state.database;
}

/**
 * 关闭数据库连接
 */
export function closeDatabaseConnection(
    databaseName: string,
    sqliteBasics: SQLiteBasics<any>
): Promise<void> | void {
    const state = DATABASE_STATE_BY_NAME.get(databaseName);
    if (state) {
        state.openConnections = state.openConnections - 1;
        if (state.openConnections === 0) {
            DATABASE_STATE_BY_NAME.delete(databaseName);
            return state.database.then(db => sqliteBasics.close(db));
        }
    }
    return;
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
        return row[4] || row[0];
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

/**
 * 事务队列
 */
export const TX_QUEUE_BY_DATABASE: WeakMap<SQLiteDatabaseClass, Promise<void>> = new WeakMap();

/**
 * 执行SQLite事务
 */
export function sqliteTransaction(
    database: SQLiteDatabaseClass,
    sqliteBasics: SQLiteBasics<any>,
    handler: () => Promise<'COMMIT' | 'ROLLBACK'>,
    /**
     * 上下文信息，用于调试
     */
    context?: any
) {
    let queue = TX_QUEUE_BY_DATABASE.get(database);
    if (!queue) {
        queue = PROMISE_RESOLVE_VOID;
    }
    queue = queue.then(async () => {
        await openSqliteTransaction(database, sqliteBasics);
        const handlerResult = await handler();
        await finishSqliteTransaction(database, sqliteBasics, handlerResult, context);
    });
    TX_QUEUE_BY_DATABASE.set(database, queue);
    return queue;
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
