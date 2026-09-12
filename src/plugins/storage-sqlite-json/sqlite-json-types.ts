import type {
  PreparedQuery,
} from '../../types/index.d.ts';

// 扩展RxErrorKey类型

export type ExtendedPreparedQuery<RxDocType> = PreparedQuery<RxDocType>;

/**
 * SQLite数据库类型
 * 由于不同的SQLite实现可能有不同的接口，
 * 我们使用any类型来表示SQLite数据库
 */
export type SQLiteDatabaseClass = any;

/**
 * SQL查询结果行
 */
export type SQLResultRow = {
  id: string;
  /**
   * 文档数据，以JSONB格式存储
   */
  data: string;
} | [string, string, number, number, string];

/**
 * SQLite基础操作接口
 * 提供与SQLite数据库交互的基本方法
 */
export type SQLiteBasics<SQLiteDatabaseType> = {
  debugId?: string;

  /**
   * 打开一个新的数据库连接
   */
  open: (name: string) => Promise<SQLiteDatabaseType>;

  /**
   * 执行查询并返回结果行
   */
  all(
    db: SQLiteDatabaseType,
    queryWithParams: SQLiteQueryWithParams
  ): Promise<SQLResultRow[]>;

  /**
   * 执行查询，不返回结果
   */
  run(
    db: SQLiteDatabaseType,
    queryWithParams: SQLiteQueryWithParams
  ): Promise<void>;

  /**
   * 设置SQLite的PRAGMA
   * @link https://www.sqlite.org/pragma.html
   */
  setPragma(
    db: SQLiteDatabaseType,
    key: string,
    value: string
  ): Promise<void>;

  /**
   * 关闭数据库连接
   */
  close(db: SQLiteDatabaseType): Promise<void>;

  /**
   * 日志模式
   * [default=WAL2]
   */
  journalMode: 'WAL' | 'WAL2' | 'DELETE' | 'TRUNCATE' | 'PERSIST' | 'MEMORY' | 'OFF' | '';
}

/**
 * SQLite存储设置
 */
export type SQLiteJSONStorageSettings = {
  sqliteBasics: SQLiteBasics<any>;

  /**
   * 是否使用JSONB格式存储数据
   * [default=true]
   */
  useJSONB?: boolean;

  /**
   * 数据库名称前缀
   */
  databaseNamePrefix?: string;

  /**
   * 查询修改器，可用于修改准备好的查询
   */
  queryModifier?: RxStorageSQLiteJSONQueryModifier<any>;

  /**
   * 日志函数
   */
  log?: typeof console.log;

  /**
   * 是否为常用字段创建索引
   * [default=true]
   */
  createIndexes?: boolean;

  /**
   * 要为其创建索引的字段
   * 如果未指定，将为主键和_meta.lwt创建索引
   */
  indexedFields?: string[];

  // 是否支持正则表达式查询
  // regexp_match(partten,text,options= "ims")
  regexSupport?: boolean;
}

/**
 * 查询修改器
 */
export type RxStorageSQLiteJSONQueryModifier<RxDocType> = (
  queryWithParams: SQLiteQueryWithParams,
  preparedQuery: SQLiteJSONPreparedQuery<RxDocType>
) => SQLiteQueryWithParams;

/**
 * 实例创建选项
 */
export type SQLiteJSONInstanceCreationOptions = {

  /**
   * 要为其创建索引的字段
   */
  indexedFields?: string[];
};

/**
 * 内部状态
 */
import type { SQLiteJSONConnectionLease } from './sqlite-json-helpers.ts';

export type SQLiteJSONInternals = {
  databasePromise: Promise<SQLiteDatabaseClass>;
  connectionLease: SQLiteJSONConnectionLease;
};

export type SQLiteJSONPreparedQuery<RxDocType> = PreparedQuery<RxDocType>;

/**
 * SQL查询及其参数
 */
export type SQLiteQueryWithParams = {
  query: string;
  /**
   * 查询参数
   */
  params: (string | number | boolean)[];

  /**
   * 上下文信息，用于调试
   */
  context: {
    method: string;
    data: any;
  };
}
