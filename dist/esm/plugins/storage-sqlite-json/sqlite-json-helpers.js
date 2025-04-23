import { PROMISE_RESOLVE_VOID, promiseWait, errorToPlainJson } from "../../index.js";
export var RX_STORAGE_NAME_SQLITE_JSON = 'sqlite-json';

/**
 * @link https://www.sqlite.org/inmemorydb.html
 */
export var SQLITE_IN_MEMORY_DB_NAME = ':memory:';

/**
 * 数据库状态
 */

/**
 * 按名称存储数据库状态
 */
var DATABASE_STATE_BY_NAME = new Map();

/**
 * 获取数据库连接
 */
export function getDatabaseConnection(sqliteBasics, databaseName) {
  var state = DATABASE_STATE_BY_NAME.get(databaseName);
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
export function closeDatabaseConnection(databaseName, sqliteBasics) {
  var state = DATABASE_STATE_BY_NAME.get(databaseName);
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
export function getDataFromResultRow(row) {
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
      var jsonStr = JSON.stringify(row);
      if (jsonStr.startsWith('{') || jsonStr.startsWith('[')) {
        return jsonStr;
      }
    } catch (err) {
      // 如果JSON.stringify失败，继续尝试其他方式
    }
  }

  // 最后尝试转换为字符串
  try {
    var str = String(row);
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
export function getSQLiteJSONInsertSQL(collectionName, primaryPath, docData) {
  // language=SQL
  var query = "\n        INSERT INTO \"" + collectionName + "\" (\n            id,\n            revision,\n            deleted,\n            lastWriteTime,\n            data\n        ) VALUES (\n            ?,\n            ?,\n            ?,\n            ?,\n            json(?)\n        );\n    ";
  var params = [docData[primaryPath], docData._rev, docData._deleted ? 1 : 0, docData._meta.lwt, JSON.stringify(docData)];
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
export function getSQLiteJSONUpdateSQL(tableName, primaryPath, writeRow) {
  var docData = writeRow.document;
  // language=SQL
  var query = "\n    UPDATE \"" + tableName + "\" SET\n        revision = ?,\n        deleted = ?,\n        lastWriteTime = ?,\n        data = json(?)\n        WHERE id = ?;\n    ";
  var params = [docData._rev, docData._deleted ? 1 : 0, docData._meta.lwt, JSON.stringify(docData), docData[primaryPath]];
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
export var TX_QUEUE_BY_DATABASE = new WeakMap();

/**
 * 执行SQLite事务
 */
export function sqliteTransaction(database, sqliteBasics, handler,
/**
 * 上下文信息，用于调试
 */
context) {
  var queue = TX_QUEUE_BY_DATABASE.get(database);
  if (!queue) {
    queue = PROMISE_RESOLVE_VOID;
  }
  queue = queue.then(async () => {
    await openSqliteTransaction(database, sqliteBasics);
    var handlerResult = await handler();
    await finishSqliteTransaction(database, sqliteBasics, handlerResult, context);
  });
  TX_QUEUE_BY_DATABASE.set(database, queue);
  return queue;
}

/**
 * 打开SQLite事务
 */
export async function openSqliteTransaction(database, sqliteBasics) {
  var openedTransaction = false;
  while (!openedTransaction) {
    try {
      await sqliteBasics.run(database, {
        query: 'BEGIN;',
        params: [],
        context: {
          method: 'openSqliteTransaction',
          data: ''
        }
      });
      openedTransaction = true;
    } catch (err) {
      console.log('open transaction error (will retry):');
      var errorAsJson = errorToPlainJson(err);
      console.log(errorAsJson);
      console.dir(err);
      if (err.message && (err.message.includes('Database is closed') || err.message.includes('API misuse'))) {
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
export function finishSqliteTransaction(database, sqliteBasics, mode,
/**
 * 上下文信息，用于调试
 */
context) {
  return sqliteBasics.run(database, {
    query: mode + ';',
    params: [],
    context: {
      method: 'finishSqliteTransaction',
      data: mode
    }
  }).catch(err => {
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
export var PARAM_KEY = '?';

/**
 * 确保参数数量正确
 */
export function ensureParamsCountIsCorrect(queryWithParams) {
  var paramsCount = queryWithParams.params.length;
  var paramKeyCount = queryWithParams.query.split(PARAM_KEY).length - 1;
  if (paramsCount !== paramKeyCount) {
    throw new Error('ensureParamsCountIsCorrect() wrong param count: ' + JSON.stringify(queryWithParams));
  }
}

/**
 * 将布尔参数转换为整数
 * SQLite不支持布尔类型，使用整数代替
 * @link https://stackoverflow.com/a/2452569/3443137
 */
export function boolParamsToInt(params) {
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
export function generateJsonPathExpression(path) {
  if (path.startsWith('$')) {
    return path;
  }
  return '$.' + path;
}

/**
 * 创建JSON索引SQL
 * 支持单字段索引和联合索引
 */
export function createJsonIndexSQL(tableName, fieldPath, indexName) {
  // 处理字段路径，可以是单个字段或字段数组（联合索引）
  var isArray = Array.isArray(fieldPath);
  var fieldPaths = isArray ? fieldPath : [fieldPath];

  // 生成索引名称
  var fieldNamePart = isArray ? fieldPaths.map(f => f.replace(/\./g, '_')).join('_') : fieldPaths[0]?.replace(/\./g, '_') || '';
  var actualIndexName = indexName || tableName + "_" + fieldNamePart + "_idx";

  // 生成索引字段列表
  var indexColumns = fieldPaths.map(field => {
    var jsonPath = generateJsonPathExpression(field);
    return "json_extract(data, '" + jsonPath + "')";
  }).join(', ');

  // language=SQL
  var query = "\n        CREATE INDEX IF NOT EXISTS \"" + actualIndexName + "\" \n        ON \"" + tableName + "\" (" + indexColumns + ");\n    ";
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
 * 将Mango查询操作符转换为SQLite JSON查询
 * 例如：{ age: { $gt: 18 } } -> "json_extract(data, '$.age') > 18"
 */
export function mangoQueryToSQLiteJSON(fieldPath, operator, value) {
  // 处理空对象作为查询条件的特殊情况
  if ((operator === '' || operator === undefined) && typeof value === 'object' && value !== null && Object.keys(value).length === 0) {
    // 空对象作为查询条件，应该返回一个始终为假的条件
    return {
      sql: '1=0',
      // 永远为假
      params: []
    };
  }
  var jsonPath = generateJsonPathExpression(fieldPath);

  // 处理不同的操作符
  switch (operator) {
    case '$eq':
      // 处理null值的特殊情况
      if (value === null) {
        return {
          sql: "json_extract(data, '" + jsonPath + "') IS NULL",
          params: []
        };
      }
      return {
        sql: "json_extract(data, '" + jsonPath + "') = ?",
        params: [value]
      };
    case '$gt':
      return {
        sql: "json_extract(data, '" + jsonPath + "') > ?",
        params: [value]
      };
    case '$gte':
      return {
        sql: "json_extract(data, '" + jsonPath + "') >= ?",
        params: [value]
      };
    case '$lt':
      return {
        sql: "json_extract(data, '" + jsonPath + "') < ?",
        params: [value]
      };
    case '$lte':
      return {
        sql: "json_extract(data, '" + jsonPath + "') <= ?",
        params: [value]
      };
    case '$ne':
      return {
        sql: "(json_extract(data, '" + jsonPath + "') IS NULL OR json_extract(data, '" + jsonPath + "') != ?)",
        params: [value]
      };
    case '$in':
      if (!Array.isArray(value) || value.length === 0) {
        return {
          sql: '0',
          // 永远为假
          params: []
        };
      }

      // 使用简单的字符串匹配方法
      var conditions = value.map(() => "json_extract(data, '" + jsonPath + "') = ? OR json_extract(data, '" + jsonPath + "') LIKE ?");
      var params = [];
      value.forEach(v => {
        // 直接匹配
        params.push(v);
        // 数组匹配 - 使用LIKE操作符
        params.push("%\"" + v + "\"%");
      });
      return {
        sql: "(" + conditions.join(' OR ') + ")",
        params
      };
    case '$nin':
      if (!Array.isArray(value) || value.length === 0) {
        return {
          sql: '1',
          // 永远为真
          params: []
        };
      }
      var ninPlaceholders = value.map(() => '?').join(', ');
      return {
        sql: "json_extract(data, '" + jsonPath + "') NOT IN (" + ninPlaceholders + ")",
        params: value
      };
    case '$exists':
      return {
        sql: value ? "json_extract(data, '" + jsonPath + "') IS NOT NULL" : "json_extract(data, '" + jsonPath + "') IS NULL",
        params: []
      };
    case '$type':
      // 将MongoDB/Mango查询类型名称映射到SQLite JSON类型名称
      if (value === 'string') {
        return {
          sql: "json_type(data, '" + jsonPath + "') = 'text'",
          params: []
        };
      } else if (value === 'number') {
        // 数字在SQLite中可能是integer或real类型
        return {
          sql: "(json_type(data, '" + jsonPath + "') = 'integer' OR json_type(data, '" + jsonPath + "') = 'real')",
          params: []
        };
      } else if (value === 'boolean') {
        return {
          sql: "json_type(data, '" + jsonPath + "') = 'boolean'",
          params: []
        };
      } else if (value === 'object') {
        return {
          sql: "json_type(data, '" + jsonPath + "') = 'object'",
          params: []
        };
      } else if (value === 'array') {
        return {
          sql: "json_type(data, '" + jsonPath + "') = 'array'",
          params: []
        };
      } else if (value === 'null') {
        return {
          sql: "json_type(data, '" + jsonPath + "') = 'null'",
          params: []
        };
      } else {
        // 默认情况，直接使用原始值
        return {
          sql: "json_type(data, '" + jsonPath + "') = ?",
          params: [value]
        };
      }
    case '$regex':
      // SQLite不直接支持正则表达式，这需要在内存中过滤
      return {
        sql: "1",
        // 我们将在内存中过滤
        params: []
      };
    case '$or':
      // 处理$or操作符，生成多个条件并用OR连接
      if (!Array.isArray(value) || value.length === 0) {
        return {
          sql: '0',
          // 空$or数组返回false
          params: []
        };
      }
      var orConditions = value.map((condition, idx) => {
        var keys = Object.keys(condition);
        if (keys.length !== 1) {
          return {
            sql: '1',
            // 复杂条件在内存中过滤
            params: []
          };
        }
        var subField = keys[0];
        var subOperator = Object.keys(condition[subField])[0];
        var subValue = condition[subField][subOperator];
        var {
          sql,
          params
        } = mangoQueryToSQLiteJSON(fieldPath + '.' + subField, subOperator, subValue);
        return {
          sql: "(" + sql + ")",
          params
        };
      });
      return {
        sql: orConditions.map(c => c.sql).join(' OR '),
        params: orConditions.flatMap(c => c.params)
      };
    case '$elemMatch':
      // 使用json_each表值函数来实现elemMatch操作
      // 这允许我们在SQLite层面处理数组元素匹配，而不是在内存中
      if (typeof value !== 'object' || value === null) {
        return {
          sql: '1=0',
          // 如果值不是对象，则返回永远为假的条件
          params: []
        };
      }

      // 处理elemMatch中的条件
      var elemConditions = [];
      var elemParams = [];
      Object.entries(value).forEach(([subField, subValue]) => {
        // 检查子值是否是包含操作符的对象
        if (typeof subValue === 'object' && subValue !== null && !Array.isArray(subValue)) {
          // 处理嵌套操作符
          Object.entries(subValue).forEach(([operator, operatorValue]) => {
            // 递归调用mangoQueryToSQLiteJSON处理嵌套操作符
            var {
              sql,
              params
            } = mangoQueryToSQLiteJSON("value." + subField,
            // 使用value作为基础路径，因为我们在json_each上下文中
            operator, operatorValue);
            elemConditions.push(sql);
            elemParams.push(...params);
          });
        } else {
          // 简单值，使用等于操作符
          elemConditions.push("json_extract(value, '$." + subField + "') = ?");
          elemParams.push(subValue);
        }
      });

      // 使用json_each函数来遍历数组元素
      return {
        sql: "EXISTS (\n          SELECT 1 FROM json_each(json_extract(data, '" + jsonPath + "'))\n          WHERE " + elemConditions.join(' AND ') + "\n        )",
        params: elemParams
      };
    case '$size':
      // $size 操作符用于匹配数组的长度
      return {
        sql: "json_array_length(json_extract(data, '" + jsonPath + "')) = ?",
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
//# sourceMappingURL=sqlite-json-helpers.js.map