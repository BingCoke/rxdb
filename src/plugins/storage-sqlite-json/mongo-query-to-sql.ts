import { generateJsonPathExpression } from './sqlite-json-helpers.ts';
import type {
  SQLiteQueryWithParams,
  ExtendedPreparedQuery
} from './sqlite-json-types.ts';
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
  primaryPath: string
  query: PreparedQuery<any>
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
export class MongoQuerySQLConverter {
  /**
   * 是否支持正则表达式
   */
  private regexSupport: boolean;


  /**
   * 不支持的操作符列表
   */
  private readonly unsupportedOperators = ['$text', '$where'];

  private query: PreparedQuery<any>;
  private tableName: string;
  private primaryPath: string;

  hasUnSpoortedOperators: boolean = false;
  /**
   * 构造函数
   * @param config 转换器配置
   */
  constructor(config: MongoQueryConverterConfig) {
    this.regexSupport = config.regexSupport || false;
    this.query = config.query;
    this.tableName = config.tableName;
    this.primaryPath = config.primaryPath;
  }

  /**
   * 将完整的Mango查询转换为SQLite JSON查询
   */
  mangoQueryToSQLiteJSONQuery<RxDocType>(
  ): SQLiteQueryWithParams {

    const query = this.query;
    const tableName = this.tableName
    const primaryPath = this.primaryPath

    const mangoQuery = query.query;
    const selector = mangoQuery.selector || {};

    // 查询构建状态
    const state: QueryState = {
      whereClauses: [],
      params: [],
      nonImplementedOperators: []
    };

    // 将selector预处理为$and形式以处理混合条件
    const transformedSelector = this.preprocessSelector(selector);

    // 处理选择器
    this.processSelector(transformedSelector, state, primaryPath);

    // 构建SQL查询
    const whereClause = state.whereClauses.length > 0
      ? `WHERE ${state.whereClauses.join(' AND ')}`
      : '';

    // 构建ORDER BY子句
    const orderByClause = this.buildOrderByClause(mangoQuery, primaryPath);

    let limitSkipClause = '';
    // 如果有不支持的操作符，则不构建LIMIT/SKIP子句
    if (!this.hasUnSpoortedOperators) {
      limitSkipClause = this.buildLimitSkipClause(mangoQuery);
    }

    // 组合完整的SQL查询
    const query_sql = `SELECT id, data FROM "${tableName}" ${whereClause} ${orderByClause} ${limitSkipClause}`;

    // 设置不支持的操作符
    if (state.nonImplementedOperators.length > 0) {
      (query as ExtendedPreparedQuery<RxDocType>).nonImplementedOperators = state.nonImplementedOperators;
    }

    return {
      query: query_sql,
      params: state.params,
      context: {
        method: 'query',
        data: query
      }
    };
  }

  /**
   * 构建 elemMatch 条件的 SQL
   * 在 json_each 上下文中，使用 value 列访问元素
   */
  private buildElemMatchCondition(
    subField: string,
    operator: string,
    opValue: any
  ): { sql: string; params: any[] } {
    const jsonPath = `json_extract(value, '$.${subField}')`;

    switch (operator) {
      case '$eq':
        if (opValue === null) {
          return { sql: `${jsonPath} IS NULL`, params: [] };
        }
        return { sql: `${jsonPath} = ?`, params: [opValue] };
      case '$gt':
        return { sql: `${jsonPath} > ?`, params: [opValue] };
      case '$gte':
        return { sql: `${jsonPath} >= ?`, params: [opValue] };
      case '$lt':
        return { sql: `${jsonPath} < ?`, params: [opValue] };
      case '$lte':
        return { sql: `${jsonPath} <= ?`, params: [opValue] };
      case '$ne':
        return { sql: `(${jsonPath} IS NULL OR ${jsonPath} != ?)`, params: [opValue] };
      case '$in':
        if (!Array.isArray(opValue) || opValue.length === 0) {
          return { sql: '0', params: [] };
        }
        const placeholders = opValue.map(() => '?').join(', ');
        return { sql: `${jsonPath} IN (${placeholders})`, params: opValue };
      case '$nin':
        if (!Array.isArray(opValue) || opValue.length === 0) {
          return { sql: '1', params: [] };
        }
        const ninPlaceholders = opValue.map(() => '?').join(', ');
        return { sql: `${jsonPath} NOT IN (${ninPlaceholders})`, params: opValue };
      case '$exists':
        return {
          sql: opValue ? `${jsonPath} IS NOT NULL` : `${jsonPath} IS NULL`,
          params: []
        };
      default:
        return { sql: '1', params: [] };
    }
  }

  /**
   * 将Mango查询操作符转换为SQLite JSON查询子句
   * 例如：{ age: { $gt: 18 } } -> "json_extract(data, '$.age') > 18"
   */
  mangoQueryToSQLiteJSON(
    fieldPath: string,
    operator: string,
    value: any
  ): { sql: string, params: any[] } {
    // 处理空对象作为查询条件的特殊情况
    if ((operator === '' || operator === undefined) &&
      typeof value === 'object' &&
      value !== null &&
      Object.keys(value).length === 0) {
      // 空对象作为查询条件，应该返回一个始终为假的条件
      return {
        sql: '1=0', // 永远为假
        params: []
      };
    }

    const jsonPath = generateJsonPathExpression(fieldPath);

    // 处理不同的操作符
    switch (operator) {
      case "$not":
        // 处理嵌套操作符
        if (typeof value === 'object' && value !== null) {
          // 检查是否包含 $regex 操作符
          if ('$regex' in value) {
            // 特殊处理 $regex 操作符: { $not: { $regex: "^p.*" } }
            const pattern = value.$regex;
            const options = value.$options || '';

            if (this.regexSupport) {
              // 使用 NOT 逻辑包装正则表达式匹配
              return {
                sql: `NOT (regexp_match(?,json_extract(data, '${jsonPath}') ,?))`,
                params: [pattern, options]
              };
            } else {
              this.hasUnSpoortedOperators = true
              // 不支持正则表达式，返回始终为真的条件
              // 内存中过滤器将处理这个
              return {
                sql: '1',
                params: []
              };
            }
          } else {
            // 处理其他嵌套操作符
            const nestedOp = Object.keys(value)[0];
            const nestedValue = value[nestedOp];

            // 递归处理嵌套操作符，然后对结果取反
            const nested = this.mangoQueryToSQLiteJSON(fieldPath, nestedOp, nestedValue);

            // 将嵌套SQL条件包装在 NOT() 中
            return {
              sql: `NOT (${nested.sql})`,
              params: nested.params
            };
          }
        }
        // 默认处理为简单值取反
        else {
          return {
            sql: `json_extract(data, '${jsonPath}') != ?`,
            params: [value]
          };
        }
      case '$eq':
        // 处理null值的特殊情况
        if (value === null) {
          return {
            sql: `json_extract(data, '${jsonPath}') IS NULL`,
            params: []
          };
        }
        return {
          sql: `json_extract(data, '${jsonPath}') = ?`,
          params: [value]
        };
      case '$gt':
        return {
          sql: `json_extract(data, '${jsonPath}') > ?`,
          params: [value]
        };
      case '$gte':
        return {
          sql: `json_extract(data, '${jsonPath}') >= ?`,
          params: [value]
        };
      case '$lt':
        return {
          sql: `json_extract(data, '${jsonPath}') < ?`,
          params: [value]
        };
      case '$lte':
        return {
          sql: `json_extract(data, '${jsonPath}') <= ?`,
          params: [value]
        };
      case '$ne':
        if (value === null) {
          return {
            sql: `(json_type(data, '${jsonPath}') IS NOT NULL AND json_type(data, '${jsonPath}') != 'null')`,
            params: []
          };
        }
        return {
          sql: `(json_extract(data, '${jsonPath}') IS NULL OR json_extract(data, '${jsonPath}') != ?)`,
          params: [value]
        };
      case '$in':
        if (!Array.isArray(value) || value.length === 0) {
          return {
            sql: '0', // 永远为假
            params: []
          };
        }

        // 使用 IN 进行直接匹配，使用 json_each + EXISTS 处理数组字段
        const inPlaceholders = value.map(() => '?').join(', ');

        return {
          sql: `(json_extract(data, '${jsonPath}') IN (${inPlaceholders}) OR (json_type(data, '${jsonPath}') = 'array' AND EXISTS (SELECT 1 FROM json_each(json_extract(data, '${jsonPath}')) WHERE value IN (${inPlaceholders}))))`,
          params: [...value, ...value]
        };
      case '$nin':
        if (!Array.isArray(value) || value.length === 0) {
          return {
            sql: '1', // 永远为真
            params: []
          };
        }

        const ninPlaceholders = value.map(() => '?').join(', ');
        return {
          sql: `(json_extract(data, '${jsonPath}') NOT IN (${ninPlaceholders}) AND (json_type(data, '${jsonPath}') != 'array' OR NOT EXISTS (SELECT 1 FROM json_each(json_extract(data, '${jsonPath}')) WHERE value IN (${ninPlaceholders}))))`,
          params: [...value, ...value]
        };
      case '$exists':
        return {
          sql: value
            ? `json_type(data, '${jsonPath}') IS NOT NULL`
            : `json_type(data, '${jsonPath}') IS NULL`,
          params: []
        };
      case '$type':
        // 将MongoDB/Mango查询类型名称映射到SQLite JSON类型名称
        if (value === 'string') {
          return {
            sql: `json_type(data, '${jsonPath}') = 'text'`,
            params: []
          };
        } else if (value === 'number') {
          // 数字在SQLite中可能是integer或real类型
          return {
            sql: `(json_type(data, '${jsonPath}') = 'integer' OR json_type(data, '${jsonPath}') = 'real')`,
            params: []
          };
        } else if (value === 'boolean') {
          return {
            sql: `json_type(data, '${jsonPath}') = 'boolean'`,
            params: []
          };
        } else if (value === 'object') {
          return {
            sql: `json_type(data, '${jsonPath}') = 'object'`,
            params: []
          };
        } else if (value === 'array') {
          return {
            sql: `json_type(data, '${jsonPath}') = 'array'`,
            params: []
          };
        } else if (value === 'null') {
          return {
            sql: `json_type(data, '${jsonPath}') = 'null'`,
            params: []
          };
        } else {
          // 默认情况，直接使用原始值
          return {
            sql: `json_type(data, '${jsonPath}') = ?`,
            params: [value]
          };
        }
      case '$elemMatch':
        // 使用json_each表值函数来实现elemMatch操作
        // 这允许我们在SQLite层面处理数组元素匹配，而不是在内存中
        if (typeof value !== 'object' || value === null) {
          return {
            sql: '1=0', // 如果值不是对象，则返回永远为假的条件
            params: []
          };
        }

        // 处理elemMatch中的条件
        const elemConditions: string[] = [];
        const elemParams: any[] = [];

        Object.entries(value).forEach(([subField, subValue]) => {
          // 检查子值是否是包含操作符的对象
          if (typeof subValue === 'object' && subValue !== null && !Array.isArray(subValue)) {
            // 处理嵌套操作符
            Object.entries(subValue).forEach(([op, opValue]) => {
              const elemSql = this.buildElemMatchCondition(subField, op, opValue);
              elemConditions.push(elemSql.sql);
              elemParams.push(...elemSql.params);
            });
          } else {
            // 简单值，使用等于操作符
            elemConditions.push(`json_extract(value, '$.${subField}') = ?`);
            elemParams.push(subValue);
          }
        });

        // 使用json_each函数来遍历数组元素，先检查字段是否为数组
        return {
          sql: `(json_type(data, '${jsonPath}') = 'array' AND EXISTS (
                        SELECT 1 FROM json_each(json_extract(data, '${jsonPath}'))
                        WHERE ${elemConditions.join(' AND ')}
                    ))`,
          params: elemParams
        };

      case '$size':
        // $size 操作符用于匹配数组的长度
        return {
          sql: `json_array_length(json_extract(data, '${jsonPath}')) = ?`,
          params: [value]
        };

      default:
        // 对于不支持的操作符，返回始终为真的条件，然后在内存中过滤
        return {
          sql: '1',
          params: []
        };
    }
  }

  /**
   * 预处理选择器，将混合了普通字段和逻辑操作符的选择器转换为$and形式
   * 用于处理像 {name: "xiaoming", $or: [{age: {$gt: 18}}, {score: {$gt: 90}}]} 这样的情况
   * 这种情况下有隐式的AND关系，我们将其转换为明确的$and形式
   */
  preprocessSelector(selector: Record<string, any>): Record<string, any> {
    // 分离逻辑操作符和普通字段
    const logicalOperators: Record<string, any> = {};
    const normalFields: Record<string, any> = {};

    Object.entries(selector).forEach(([key, value]) => {
      if (key.startsWith('$')) {
        logicalOperators[key] = value;
      } else {
        normalFields[key] = value;
      }
    });

    // 如果只有逻辑操作符或只有普通字段，不需要转换
    if (Object.keys(logicalOperators).length === 0 || Object.keys(normalFields).length === 0) {
      return selector;
    }

    // 构建$and数组
    const andConditions: Record<string, any>[] = [
      { ...logicalOperators } // 包含所有逻辑操作符
    ];

    // 为每个普通字段创建一个条件
    Object.entries(normalFields).forEach(([key, value]) => {
      andConditions.push({ [key]: value });
    });

    return { $and: andConditions };
  }

  /**
   * 处理查询选择器
   */
  processSelector(
    selector: Record<string, any>,
    state: QueryState,
    primaryPath: string
  ): void {
    // 处理顶级逻辑操作符
    if (this.processLogicalOperators(selector, state, primaryPath)) {
      return;
    }

    // 处理常规字段
    Object.entries(selector).forEach(([field, condition]) => {
      if (field === '_id') {
        field = primaryPath;
      }

      this.processFieldCondition(field, condition, state);
    });
  }

  /**
   * 处理逻辑操作符($and, $or, $nor, $not)
   * 返回是否处理了操作符
   */
  processLogicalOperators(
    selector: Record<string, any>,
    state: QueryState,
    primaryPath: string
  ): boolean {
    // 处理 $and 操作符
    if (selector.$and && Array.isArray(selector.$and) && selector.$and.length > 0) {
      this.processLogicalOperator(selector.$and, ' AND ', state, primaryPath);
      return true;
    }

    // 处理 $or 操作符
    if (selector.$or && Array.isArray(selector.$or) && selector.$or.length > 0) {
      this.processLogicalOperator(selector.$or, ' OR ', state, primaryPath);
      return true;
    }

    // 处理 $nor 操作符
    if (selector.$nor && Array.isArray(selector.$nor) && selector.$nor.length > 0) {
      const norState: QueryState = {
        whereClauses: [],
        params: [],
        nonImplementedOperators: state.nonImplementedOperators
      };

      this.processLogicalOperator(selector.$nor, ' OR ', norState, primaryPath);

      if (norState.whereClauses.length > 0) {
        state.whereClauses.push(`NOT (${norState.whereClauses.join(' AND ')})`);
        state.params.push(...norState.params);
      }

      return true;
    }

    return false;
  }

  /**
   * 处理逻辑操作符的条件数组
   */
  processLogicalOperator(
    conditions: Record<string, any>[],
    joinOperator: string,
    state: QueryState,
    primaryPath: string
  ): void {
    const clauses: string[] = [];
    const clauseParams: any[] = [];

    conditions.forEach(condition => {
      const conditionState: QueryState = {
        whereClauses: [],
        params: [],
        nonImplementedOperators: state.nonImplementedOperators
      };

      this.processSelector(condition, conditionState, primaryPath);

      if (conditionState.whereClauses.length > 0) {
        clauses.push(`(${conditionState.whereClauses.join(' AND ')})`);
        clauseParams.push(...conditionState.params);
      }
    });

    if (clauses.length > 0) {
      state.whereClauses.push(`(${clauses.join(joinOperator)})`);
      state.params.push(...clauseParams);
    }
  }

  /**
   * 处理字段条件
   */
  processFieldCondition(
    field: string,
    condition: any,
    state: QueryState
  ): void {
    // 如果条件是简单值，视为 $eq 操作符
    if (typeof condition !== 'object' || condition === null) {
      const { sql, params } = this.mangoQueryToSQLiteJSON(field, '$eq', condition);
      state.whereClauses.push(sql);
      state.params.push(...params);
      return;
    }

    // 处理空对象条件
    if (Object.keys(condition).length === 0) {
      state.whereClauses.push('1=0');
      return;
    }

    // 检查是否有正则表达式相关操作符
    this.processRegexOperators(field, condition, state);

    // 处理其他操作符
    Object.entries(condition).forEach(([operator, value]) => {
      // 跳过已处理的正则表达式选项
      if (operator === '$regex' || operator === '$options') {
        return;
      }

      const { sql, params } = this.mangoQueryToSQLiteJSON(field, operator, value);
      state.whereClauses.push(sql);
      state.params.push(...params);
    });
  }

  /**
   * 处理正则表达式操作符
   * 例如: { field: { $regex: "pattern" } }
   */
  processRegexOperators(
    field: string,
    condition: Record<string, any>,
    state: QueryState
  ): boolean {
    // 检查是否存在正则表达式操作符
    if (condition.$regex === undefined) {
      return false;
    }

    // 提取正则表达式和选项
    const pattern = condition.$regex;
    const options = condition.$options || '';

    // 根据SQLite是否支持正则表达式决定处理方式
    if (this.regexSupport) {
      // SQLite支持正则表达式
      const jsonPath = `json_extract(data, '$.${field}')`;

      // 字符串格式: { name: { $regex: 'acme.*corp', $options: 'i' } }
      state.whereClauses.push(`regexp_match(?, ${jsonPath}, ?)`);
      state.params.push(pattern, options);
    } else {
      this.hasUnSpoortedOperators = true
    }

    return true;
  }

  /**
   * 构建ORDER BY子句
   */
  buildOrderByClause<RxDocType>(
    mangoQuery: FilledMangoQuery<RxDocType>,
    primaryPath: string
  ): string {
    if (!mangoQuery.sort || !Array.isArray(mangoQuery.sort) || mangoQuery.sort.length === 0) {
      return '';
    }

    const sortParts = mangoQuery.sort
      .map((sortObj) => {
        const field = Object.keys(sortObj)[0];
        if (!field) {
          return '';
        }

        const direction = sortObj[field] === 'desc' ? 'DESC' : 'ASC';
        const jsonPath = (field === '_id' || field === primaryPath) ? 'id' : `json_extract(data, '$.${field}')`;

        return `${jsonPath} ${direction}`;
      })
      .filter((part) => part !== '');

    return sortParts.length > 0 ? `ORDER BY ${sortParts.join(', ')}` : '';
  }

  /**
   * 构建LIMIT和SKIP子句
   */
  buildLimitSkipClause<RxDocType>(mangoQuery: FilledMangoQuery<RxDocType>): string {
    if (!mangoQuery.limit && !mangoQuery.skip) {
      return '';
    }

    if (mangoQuery.limit) {
      return mangoQuery.skip
        ? `LIMIT ${mangoQuery.limit} OFFSET ${mangoQuery.skip}`
        : `LIMIT ${mangoQuery.limit}`;
    }

    return mangoQuery.skip ? `LIMIT -1 OFFSET ${mangoQuery.skip}` : '';
  }
}



// 导出用于创建自定义配置实例的工厂函数
export function createMongoQuerySQLConverter(config: MongoQueryConverterConfig): MongoQuerySQLConverter {
  return new MongoQuerySQLConverter(config);
}
