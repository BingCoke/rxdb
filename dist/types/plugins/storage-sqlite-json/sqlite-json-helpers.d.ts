import type { BulkWriteRow, RxDocumentData } from '../../types/index.d.ts';
import type { SQLResultRow, SQLiteBasics, SQLiteDatabaseClass, SQLiteQueryWithParams } from './sqlite-json-types.ts';
export declare const RX_STORAGE_NAME_SQLITE_JSON = "sqlite-json";
/**
 * @link https://www.sqlite.org/inmemorydb.html
 */
export declare const SQLITE_IN_MEMORY_DB_NAME = ":memory:";
/**
 * 获取数据库连接
 */
export declare function getDatabaseConnection(sqliteBasics: SQLiteBasics<any>, databaseName: string): Promise<SQLiteDatabaseClass>;
/**
 * 关闭数据库连接
 */
export declare function closeDatabaseConnection(databaseName: string, sqliteBasics: SQLiteBasics<any>): Promise<void> | void;
/**
 * 从结果行中获取数据
 */
export declare function getDataFromResultRow(row: SQLResultRow): string;
/**
 * 生成SQLite插入SQL
 */
export declare function getSQLiteJSONInsertSQL<RxDocType>(collectionName: string, primaryPath: keyof RxDocType, docData: RxDocumentData<RxDocType>): SQLiteQueryWithParams;
/**
 * 生成SQLite更新SQL
 */
export declare function getSQLiteJSONUpdateSQL<RxDocType>(tableName: string, primaryPath: keyof RxDocType, writeRow: BulkWriteRow<RxDocType>): SQLiteQueryWithParams;
/**
 * 事务队列
 */
export declare const TX_QUEUE_BY_DATABASE: WeakMap<SQLiteDatabaseClass, Promise<void>>;
/**
 * 执行SQLite事务
 */
export declare function sqliteTransaction(database: SQLiteDatabaseClass, sqliteBasics: SQLiteBasics<any>, handler: () => Promise<'COMMIT' | 'ROLLBACK'>, 
/**
 * 上下文信息，用于调试
 */
context?: any): Promise<void>;
/**
 * 打开SQLite事务
 */
export declare function openSqliteTransaction(database: SQLiteDatabaseClass, sqliteBasics: SQLiteBasics<any>): Promise<void>;
/**
 * 完成SQLite事务
 */
export declare function finishSqliteTransaction(database: SQLiteDatabaseClass, sqliteBasics: SQLiteBasics<any>, mode: 'COMMIT' | 'ROLLBACK', 
/**
 * 上下文信息，用于调试
 */
context?: any): Promise<void>;
/**
 * 参数占位符
 */
export declare const PARAM_KEY = "?";
/**
 * 确保参数数量正确
 */
export declare function ensureParamsCountIsCorrect(queryWithParams: SQLiteQueryWithParams): void;
/**
 * 将布尔参数转换为整数
 * SQLite不支持布尔类型，使用整数代替
 * @link https://stackoverflow.com/a/2452569/3443137
 */
export declare function boolParamsToInt(params: any[]): any[];
/**
 * 生成JSON路径表达式
 * 将JavaScript路径转换为SQLite JSON路径
 * 例如：'user.name' -> '$.user.name'
 */
export declare function generateJsonPathExpression(path: string): string;
/**
 * 创建JSON索引SQL
 * 支持单字段索引和联合索引
 */
export declare function createJsonIndexSQL(tableName: string, fieldPath: string | string[], indexName?: string): SQLiteQueryWithParams;
