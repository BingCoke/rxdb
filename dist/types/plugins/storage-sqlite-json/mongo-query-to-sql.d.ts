import type { SQLiteQueryWithParams } from './sqlite-json-types.ts';
import type { PreparedQuery } from '../../types/index.d.ts';
import { FilledMangoQuery } from '../../types/rx-storage.interface.ts';
/**
 * MongoDB查询转SQL转换器配置
 */
export interface MongoQueryConverterConfig {
    /**
     * 是否支持正则表达式（SQLite扩展）
     */
    regexSupport?: boolean;
    tableName: string;
    primaryPath: string;
    query: PreparedQuery<any>;
}
/**
 * 查询状态接口
 */
export interface QueryState {
    whereClauses: string[];
    params: any[];
    nonImplementedOperators: string[];
}
/**
 * MongoDB查询转SQL转换器
 * 负责将MongoDB风格的查询转换为SQLite JSON查询
 */
export declare class MongoQuerySQLConverter {
    /**
     * 是否支持正则表达式
     */
    private regexSupport;
    /**
     * 不支持的操作符列表
     */
    private readonly unsupportedOperators;
    private query;
    private tableName;
    private primaryPath;
    hasUnSpoortedOperators: boolean;
    /**
     * 构造函数
     * @param config 转换器配置
     */
    constructor(config: MongoQueryConverterConfig);
    /**
     * 将完整的Mango查询转换为SQLite JSON查询
     */
    mangoQueryToSQLiteJSONQuery<RxDocType>(): SQLiteQueryWithParams;
    /**
     * 将Mango查询操作符转换为SQLite JSON查询子句
     * 例如：{ age: { $gt: 18 } } -> "json_extract(data, '$.age') > 18"
     */
    mangoQueryToSQLiteJSON(fieldPath: string, operator: string, value: any): {
        sql: string;
        params: any[];
    };
    /**
     * 预处理选择器，将混合了普通字段和逻辑操作符的选择器转换为$and形式
     * 用于处理像 {name: "xiaoming", $or: [{age: {$gt: 18}}, {score: {$gt: 90}}]} 这样的情况
     * 这种情况下有隐式的AND关系，我们将其转换为明确的$and形式
     */
    preprocessSelector(selector: Record<string, any>): Record<string, any>;
    /**
     * 处理查询选择器
     */
    processSelector(selector: Record<string, any>, state: QueryState, primaryPath: string): void;
    /**
     * 处理逻辑操作符($and, $or, $nor, $not)
     * 返回是否处理了操作符
     */
    processLogicalOperators(selector: Record<string, any>, state: QueryState, primaryPath: string): boolean;
    /**
     * 处理逻辑操作符的条件数组
     */
    processLogicalOperator(conditions: Record<string, any>[], joinOperator: string, state: QueryState, primaryPath: string): void;
    /**
     * 处理字段条件
     */
    processFieldCondition(field: string, condition: any, state: QueryState): void;
    /**
     * 处理正则表达式操作符
     * 例如: { field: { $regex: "pattern" } }
     */
    processRegexOperators(field: string, condition: Record<string, any>, state: QueryState): boolean;
    /**
     * 构建ORDER BY子句
     */
    buildOrderByClause<RxDocType>(mangoQuery: FilledMangoQuery<RxDocType>, primaryPath: string): string;
    /**
     * 构建LIMIT和SKIP子句
     */
    buildLimitSkipClause<RxDocType>(mangoQuery: FilledMangoQuery<RxDocType>): string;
}
export declare function createMongoQuerySQLConverter(config: MongoQueryConverterConfig): MongoQuerySQLConverter;
