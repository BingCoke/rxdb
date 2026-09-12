import { generateJsonPathExpression, boolParamsToInt, getMultiKeyIndexTableName } from './sqlite-json-helpers.ts';
import type {
  SQLiteQueryWithParams
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
  /**
   * 数组类型的字段路径集合，用于优化查询
   * 如果提供，非数组字段将跳过数组检查逻辑
   */
  arrayFields?: Set<string>;
}

/**
 * 查询状态接口
 */
export interface QueryState {
  whereClauses: string[];
  params: any[];
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

  private query: PreparedQuery<any>;
  private tableName: string;
  private primaryPath: string;
  private arrayFields?: Set<string>;

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
    this.arrayFields = config.arrayFields;
  }

  /**
   * 判断字段是否可能为数组类型
   * 如果未提供 arrayFields，保守地假设可能是数组
   */
  private isArrayField(fieldPath: string): boolean {
    if (!this.arrayFields) return false;
    return this.arrayFields.has(fieldPath);
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
      params: []
    };

    // 将selector预处理为$and形式以处理混合条件
    const transformedSelector = this.preprocessSelector(selector);

    // 处理选择器
    this.processSelector(transformedSelector, state, primaryPath);

    // 构建SQL查询
    const whereClause = !this.hasUnSpoortedOperators && state.whereClauses.length > 0
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

    return {
      query: query_sql,
      params: this.hasUnSpoortedOperators ? [] : boolParamsToInt(state.params),
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
        if (opValue === null) {
          return { sql: `${jsonPath} IS NOT NULL`, params: [] };
        }
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
        this.hasUnSpoortedOperators = true;
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
    const jsonPath = generateJsonPathExpression(fieldPath);

    // 处理空对象作为查询条件的特殊情况
    // 在 MongoDB 中 { field: {} } 匹配 field 值为空对象的文档
    if ((operator === '' || operator === undefined) &&
      typeof value === 'object' &&
      value !== null &&
      Object.keys(value).length === 0) {
      return {
        sql: `json_extract(data, '${jsonPath}') = json('{}')`,
        params: []
      };
    }

    // 处理不同的操作符
    switch (operator) {
      case "$not":
        this.hasUnSpoortedOperators = true;
        return {
          sql: '1',
          params: []
        };
      case '$eq':
        // 处理null值的特殊情况
        // MongoDB 中 { field: null } 匹配: field 值为 null 或 field 不存在
        // 对数组字段，还需要匹配数组中包含 null 元素的文档
        if (value === null) {
          if (this.isArrayField(fieldPath)) {
            const mkiTable = getMultiKeyIndexTableName(this.tableName, fieldPath);
            return {
              sql: `(json_extract(data, '${jsonPath}') IS NULL OR json_type(data, '${jsonPath}') IS NULL OR id IN (SELECT doc_id FROM "${mkiTable}" WHERE value = json('null')))`,
              params: []
            };
          }
          return {
            sql: `(json_extract(data, '${jsonPath}') IS NULL OR json_type(data, '${jsonPath}') IS NULL)`,
            params: []
          };
        }
        // MongoDB 数组查询语义:
        // - { tags: '1' } (标量值) → 数组包含 '1'，使用多键索引
        // - { tags: ['1','2'] } (数组值) → 精确匹配整个数组，比较 JSON
        if (this.isArrayField(fieldPath)) {
          if (Array.isArray(value)) {
            // 精确匹配整个数组 - 直接比较 JSON
            return {
              sql: `json_extract(data, '${jsonPath}') = json(?)`,
              params: [JSON.stringify(value)]
            };
          } else {
            // 包含单个值 - 使用多键索引
            const mkiTable = getMultiKeyIndexTableName(this.tableName, fieldPath);
            return {
              sql: `id IN (SELECT doc_id FROM "${mkiTable}" WHERE value = json(?))`,
              params: [JSON.stringify(value)]
            };
          }
        }
        if (Array.isArray(value)) {
          return {
            sql: `json_extract(data, '${jsonPath}') = json(?)`,
            params: [JSON.stringify(value)]
          };
        }
        return {
          sql: `json_extract(data, '${jsonPath}') = ?`,
          params: [value]
        };
      case '$gt':
        // MongoDB 中对数组字段，只要有任意元素满足条件即匹配
        // 使用多键索引
        if (this.isArrayField(fieldPath)) {
          const mkiTableGt = getMultiKeyIndexTableName(this.tableName, fieldPath);
          return {
            sql: `id IN (SELECT doc_id FROM "${mkiTableGt}" WHERE value > json(?))`,
            params: [JSON.stringify(value)]
          };
        }
        return {
          sql: `json_extract(data, '${jsonPath}') > ?`,
          params: [value]
        };
      case '$gte':
        if (this.isArrayField(fieldPath)) {
          const mkiTableGte = getMultiKeyIndexTableName(this.tableName, fieldPath);
          return {
            sql: `id IN (SELECT doc_id FROM "${mkiTableGte}" WHERE value >= json(?))`,
            params: [JSON.stringify(value)]
          };
        }
        return {
          sql: `json_extract(data, '${jsonPath}') >= ?`,
          params: [value]
        };
      case '$lt':
        if (this.isArrayField(fieldPath)) {
          const mkiTableLt = getMultiKeyIndexTableName(this.tableName, fieldPath);
          return {
            sql: `id IN (SELECT doc_id FROM "${mkiTableLt}" WHERE value < json(?))`,
            params: [JSON.stringify(value)]
          };
        }
        return {
          sql: `json_extract(data, '${jsonPath}') < ?`,
          params: [value]
        };
      case '$lte':
        if (this.isArrayField(fieldPath)) {
          const mkiTableLte = getMultiKeyIndexTableName(this.tableName, fieldPath);
          return {
            sql: `id IN (SELECT doc_id FROM "${mkiTableLte}" WHERE value <= json(?))`,
            params: [JSON.stringify(value)]
          };
        }
        return {
          sql: `json_extract(data, '${jsonPath}') <= ?`,
          params: [value]
        };
      case '$ne':
        if (value === null) {
          // 对数组字段，需要额外排除数组中包含 null 元素的文档
          if (this.isArrayField(fieldPath)) {
            const mkiTable = getMultiKeyIndexTableName(this.tableName, fieldPath);
            return {
              sql: `(json_type(data, '${jsonPath}') IS NOT NULL AND json_type(data, '${jsonPath}') != 'null' AND id NOT IN (SELECT doc_id FROM "${mkiTable}" WHERE value = json('null')))`,
              params: []
            };
          }
          return {
            sql: `(json_type(data, '${jsonPath}') IS NOT NULL AND json_type(data, '${jsonPath}') != 'null')`,
            params: []
          };
        }
        // MongoDB 中 $ne 对数组字段会匹配数组中不包含该值的文档
        // 使用多键索引: NOT IN (SELECT doc_id FROM mki_table WHERE value = json(?))
        if (this.isArrayField(fieldPath)) {
          const mkiTableNe = getMultiKeyIndexTableName(this.tableName, fieldPath);
          return {
            sql: `id NOT IN (SELECT doc_id FROM "${mkiTableNe}" WHERE value = json(?))`,
            params: [JSON.stringify(value)]
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

        // 分离 null 和非 null 值，因为 SQL 中 IN (NULL) 不能正确匹配 null
        const hasNull = value.includes(null);
        const nonNullValues = value.filter(v => v !== null);

        if (nonNullValues.length === 0) {
          // 只有 null
          // 对数组字段，还需要检查数组中是否包含 null 元素
          if (this.isArrayField(fieldPath)) {
            const mkiTableIn = getMultiKeyIndexTableName(this.tableName, fieldPath);
            return {
              sql: `(json_extract(data, '${jsonPath}') IS NULL OR json_type(data, '${jsonPath}') IS NULL OR id IN (SELECT doc_id FROM "${mkiTableIn}" WHERE value = json('null')))`,
              params: []
            };
          }
          return {
            sql: `(json_extract(data, '${jsonPath}') IS NULL OR json_type(data, '${jsonPath}') IS NULL)`,
            params: []
          };
        }

        const nullCheck = hasNull ? ` OR json_extract(data, '${jsonPath}') IS NULL OR json_type(data, '${jsonPath}') IS NULL` : '';

        if (this.isArrayField(fieldPath)) {
          // 使用多键索引进行 $in 查询，使用 json(?) 保留类型
          const mkiTableIn = getMultiKeyIndexTableName(this.tableName, fieldPath);
          const inPlaceholders = nonNullValues.map(() => 'json(?)').join(', ');
          const jsonValues = nonNullValues.map(v => JSON.stringify(v));
          if (hasNull) {
            // 如果包含 null，需要额外检查数组中包含 null 元素的情况
            // 将 json('null') 添加到 IN 列表中，同时检查字段本身是否为 NULL
            const allPlaceholders = nonNullValues.length > 0
              ? `${inPlaceholders}, json('null')`
              : "json('null')";
            return {
              sql: `(id IN (SELECT doc_id FROM "${mkiTableIn}" WHERE value IN (${allPlaceholders})) OR json_extract(data, '${jsonPath}') IS NULL OR json_type(data, '${jsonPath}') IS NULL)`,
              params: jsonValues
            };
          }
          return {
            sql: `id IN (SELECT doc_id FROM "${mkiTableIn}" WHERE value IN (${inPlaceholders}))`,
            params: jsonValues
          };
        }
        // 非数组字段使用普通比较
        const inPlaceholders = nonNullValues.map(() => '?').join(', ');
        return {
          sql: `(json_extract(data, '${jsonPath}') IN (${inPlaceholders})${nullCheck})`,
          params: nonNullValues
        };
      case '$nin':
        if (!Array.isArray(value) || value.length === 0) {
          return {
            sql: '1', // 永远为真
            params: []
          };
        }

        // 分离 null 和非 null 值
        const ninHasNull = value.includes(null);
        const ninNonNullValues = value.filter(v => v !== null);

        if (ninNonNullValues.length === 0) {
          // 只有 null，排除 null 值和字段不存在的文档
          // 对数组字段，还需要排除数组中包含 null 元素的文档
          if (this.isArrayField(fieldPath)) {
            const mkiTableNin = getMultiKeyIndexTableName(this.tableName, fieldPath);
            return {
              sql: `(json_extract(data, '${jsonPath}') IS NOT NULL AND json_type(data, '${jsonPath}') IS NOT NULL AND id NOT IN (SELECT doc_id FROM "${mkiTableNin}" WHERE value = json('null')))`,
              params: []
            };
          }
          return {
            sql: `(json_extract(data, '${jsonPath}') IS NOT NULL AND json_type(data, '${jsonPath}') IS NOT NULL)`,
            params: []
          };
        }

        const ninNullCheck = ninHasNull ? ` AND json_extract(data, '${jsonPath}') IS NOT NULL AND json_type(data, '${jsonPath}') IS NOT NULL` : '';

        if (this.isArrayField(fieldPath)) {
          // 使用多键索引进行 $nin 查询，使用 json(?) 保留类型
          const mkiTableNin = getMultiKeyIndexTableName(this.tableName, fieldPath);
          const ninPlaceholders = ninNonNullValues.map(() => 'json(?)').join(', ');
          const ninJsonValues = ninNonNullValues.map(v => JSON.stringify(v));
          if (ninHasNull) {
            // 如果包含 null，需要排除数组中包含 null 元素的文档
            // 将 json('null') 添加到 IN 列表中，同时排除字段本身为 NULL 的情况
            const allPlaceholders = ninNonNullValues.length > 0
              ? `${ninPlaceholders}, json('null')`
              : "json('null')";
            return {
              sql: `(id NOT IN (SELECT doc_id FROM "${mkiTableNin}" WHERE value IN (${allPlaceholders})) AND json_extract(data, '${jsonPath}') IS NOT NULL AND json_type(data, '${jsonPath}') IS NOT NULL)`,
              params: ninJsonValues
            };
          }
          return {
            sql: `id NOT IN (SELECT doc_id FROM "${mkiTableNin}" WHERE value IN (${ninPlaceholders}))`,
            params: ninJsonValues
          };
        }
        // 非数组字段使用普通比较
        const ninPlaceholders = ninNonNullValues.map(() => '?').join(', ');
        return {
          sql: `(json_extract(data, '${jsonPath}') NOT IN (${ninPlaceholders})${ninNullCheck})`,
          params: ninNonNullValues
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
            sql: `json_type(data, '${jsonPath}') IN ('true', 'false')`,
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
        if (Object.keys(value).some(key => key.startsWith('$'))) {
          this.hasUnSpoortedOperators = true;
          return {
            sql: '1',
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
        this.hasUnSpoortedOperators = true;
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
    // 处理逻辑操作符
    if (this.processLogicalOperators(this.preprocessSelector(selector), state, primaryPath)) {
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
    let processed = false;

    // 处理 $and 操作符
    if (selector.$and && Array.isArray(selector.$and) && selector.$and.length > 0) {
      this.processLogicalOperator(selector.$and, ' AND ', state, primaryPath);
      processed = true;
    }

    // 处理 $or 操作符
    if (selector.$or && Array.isArray(selector.$or) && selector.$or.length > 0) {
      this.processLogicalOperator(selector.$or, ' OR ', state, primaryPath);
      processed = true;
    }

    // 处理 $nor 操作符: NOT(cond1) AND NOT(cond2) AND ... 等价于 NOT(cond1 OR cond2 OR ...)
    if (selector.$nor && Array.isArray(selector.$nor) && selector.$nor.length > 0) {
      const norClauses: string[] = [];
      const norParams: any[] = [];

      selector.$nor.forEach(condition => {
        const conditionState: QueryState = {
          whereClauses: [],
          params: []
        };
        this.processSelector(condition, conditionState, primaryPath);

        if (conditionState.whereClauses.length > 0) {
          norClauses.push(`(${conditionState.whereClauses.join(' AND ')})`);
          norParams.push(...conditionState.params);
        }
      });

      if (norClauses.length > 0) {
        state.whereClauses.push(`NOT (${norClauses.join(' OR ')})`);
        state.params.push(...norParams);
      }

      processed = true;
    }

    return processed;
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
        params: []
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
    // 如果条件是简单值或数组，视为 $eq 操作符
    // 数组在 MongoDB 语义中表示精确匹配整个数组
    if (typeof condition !== 'object' || condition === null || Array.isArray(condition)) {
      const { sql, params } = this.mangoQueryToSQLiteJSON(field, '$eq', condition);
      state.whereClauses.push(sql);
      state.params.push(...params);
      return;
    }

    // 处理空对象条件 - 匹配 field 值为空对象的文档
    if (Object.keys(condition).length === 0) {
      const { sql, params } = this.mangoQueryToSQLiteJSON(field, '', condition);
      state.whereClauses.push(sql);
      state.params.push(...params);
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
      const jsonPath = `json_extract(data, '${generateJsonPathExpression(field)}')`;

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
    if (mangoQuery.limit === undefined && !mangoQuery.skip) {
      return '';
    }

    if (mangoQuery.limit !== undefined) {
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
