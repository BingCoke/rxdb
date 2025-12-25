"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
var _exportNames = {
  RxStorageSQLiteJSON: true,
  getRxStorageSQLiteJSON: true,
  RxStorageInstanceSQLiteJSON: true,
  createSQLiteJSONStorageInstance: true
};
exports.RxStorageSQLiteJSON = exports.RxStorageInstanceSQLiteJSON = void 0;
exports.createSQLiteJSONStorageInstance = createSQLiteJSONStorageInstance;
exports.getRxStorageSQLiteJSON = getRxStorageSQLiteJSON;
var _index = require("../../index.js");
var _rxjs = require("rxjs");
var _sqliteJsonHelpers = require("./sqlite-json-helpers.js");
Object.keys(_sqliteJsonHelpers).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _sqliteJsonHelpers[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _sqliteJsonHelpers[key];
    }
  });
});
var _mongoQueryToSql = require("./mongo-query-to-sql.js");
Object.keys(_mongoQueryToSql).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _mongoQueryToSql[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _mongoQueryToSql[key];
    }
  });
});
var _sqliteJsonTypes = require("./sqlite-json-types.js");
Object.keys(_sqliteJsonTypes).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _sqliteJsonTypes[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _sqliteJsonTypes[key];
    }
  });
});
var RxStorageSQLiteJSON = exports.RxStorageSQLiteJSON = /*#__PURE__*/function () {
  function RxStorageSQLiteJSON(settings) {
    this.name = _sqliteJsonHelpers.RX_STORAGE_NAME_SQLITE_JSON;
    this.rxdbVersion = _index.RXDB_VERSION;
    this.settings = settings;
  }

  /**
   * 创建存储实例
   */
  var _proto = RxStorageSQLiteJSON.prototype;
  _proto.createStorageInstance = async function createStorageInstance(params) {
    (0, _index.ensureRxStorageInstanceParamsAreCorrect)(params);
    return createSQLiteJSONStorageInstance(this, params, this.settings);
  };
  return RxStorageSQLiteJSON;
}();
/**
 * 获取SQLite JSON存储实例
 */
function getRxStorageSQLiteJSON(settings) {
  var storage = new RxStorageSQLiteJSON(settings);
  return storage;
}
var instanceId = 0;

/**
 * SQLite JSON存储实例
 * 利用SQLite的JSON功能实现高效的RxStorage
 */
var RxStorageInstanceSQLiteJSON = exports.RxStorageInstanceSQLiteJSON = /*#__PURE__*/function () {
  function RxStorageInstanceSQLiteJSON(storage, databaseName, collectionName, schema, internals, options, settings, tableName, devMode, internalDatabaseName) {
    this.changes$ = new _rxjs.Subject();
    this.instanceId = instanceId++;
    this.openWriteCount$ = new _rxjs.BehaviorSubject(0);
    this.storage = storage;
    this.databaseName = databaseName;
    this.collectionName = collectionName;
    this.schema = schema;
    this.internals = internals;
    this.options = options;
    this.settings = settings;
    this.tableName = tableName;
    this.devMode = devMode;
    this.internalDatabaseName = internalDatabaseName;
    this.sqliteBasics = storage.settings.sqliteBasics;
    this.primaryPath = (0, _index.getPrimaryFieldOfPrimaryKey)(this.schema.primaryKey);
    this.arrayFields = this.extractArrayFields(schema);
  }

  /**
   * 从 schema 中提取数组类型的字段路径
   */
  var _proto2 = RxStorageInstanceSQLiteJSON.prototype;
  _proto2.extractArrayFields = function extractArrayFields(schema) {
    var arrayFields = new Set();
    var properties = schema.properties || {};
    var isArrayType = type => {
      if (type === 'array') return true;
      if (Array.isArray(type)) return type.includes('array');
      return false;
    };
    var traverse = (obj, prefix) => {
      for (var [key, value] of Object.entries(obj)) {
        var path = prefix ? prefix + "." + key : key;
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
   * 执行SQL查询，不返回结果
   */;
  _proto2.run = function run(db, queryWithParams) {
    if (this.devMode) {
      (0, _sqliteJsonHelpers.ensureParamsCountIsCorrect)(queryWithParams);
    }
    return this.sqliteBasics.run(db, queryWithParams);
  }

  /**
   * 执行SQL查询，返回结果
   */;
  _proto2.all = function all(db, queryWithParams) {
    if (this.devMode) {
      (0, _sqliteJsonHelpers.ensureParamsCountIsCorrect)(queryWithParams);
    }
    return this.sqliteBasics.all(db, queryWithParams);
  }

  /**
   * 批量写入文档
   */;
  _proto2.bulkWrite = async function bulkWrite(documentWrites, context) {
    this.openWriteCount$.next(this.openWriteCount$.getValue() + 1);
    var database = await this.internals.databasePromise;
    var ret = {
      error: []
    };
    var writePromises = [];
    var categorized = {};
    await (0, _sqliteJsonHelpers.sqliteTransaction)(database, this.sqliteBasics, async () => {
      if (this.closed) {
        this.openWriteCount$.next(this.openWriteCount$.getValue() - 1);
        throw new Error('SQLiteJSON.bulkWrite(' + context + ') already closed ' + this.tableName + ' context: ' + context);
      }

      // 只查询需要操作的文档ID
      var docIds = documentWrites.map(d => d.document[this.primaryPath]);
      var result = await this.findDocumentsById(docIds, true);
      // 执行查询

      // 构建文档映射
      var docsInDb = new Map();
      result.forEach(doc => {
        var id = doc[this.primaryPath];
        if (id) {
          docsInDb.set(id, doc);
        }
      });

      // 分类批量写入行
      categorized = (0, _index.categorizeBulkWriteRows)(this, this.primaryPath, docsInDb, documentWrites, context);
      ret.error = categorized.errors;

      // 执行插入操作
      categorized.bulkInsertDocs.forEach(row => {
        var insertQuery = (0, _sqliteJsonHelpers.getSQLiteJSONInsertSQL)(this.tableName, this.primaryPath, row.document);
        writePromises.push(this.run(database, insertQuery));
      });

      // 执行更新操作
      categorized.bulkUpdateDocs.forEach(row => {
        var updateQuery = (0, _sqliteJsonHelpers.getSQLiteJSONUpdateSQL)(this.tableName, this.primaryPath, row);
        writePromises.push(this.run(database, updateQuery));
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
    }, {
      databaseName: this.databaseName,
      collectionName: this.collectionName
    });

    // 发送变更事件
    if (categorized && categorized.eventBulk.events.length > 0) {
      var lastState = (0, _index.ensureNotFalsy)(categorized.newestRow).document;
      categorized.eventBulk.checkpoint = {
        id: lastState[this.primaryPath],
        lwt: lastState._meta.lwt
      };
      this.changes$.next(categorized.eventBulk);
    }
    return ret;
  }

  ///**
  // * 将Mango查询转换为SQLite JSON查询
  // * 利用SQLite的JSON函数高效查询嵌套数据
  // */
  //private mangoQueryToSQLiteJSONQuery<RxDocType>(
  //    query: PreparedQuery<RxDocType>
  //): SQLiteQueryWithParams {
  //    // 创建一个新的转换器实例，并配置正则表达式支持
  //    const converter = createMongoQuerySQLConverter({
  //        regexSupport: this.settings.regexSupport || false,
  //        query,
  //        tableName: this.tableName,
  //
  //        primaryPath: this.primaryPath as string
  //    });
  //
  //    // 使用转换器进行查询转换
  //    return converter.mangoQueryToSQLiteJSONQuery();
  //}
  //
  //// 这些私有方法已移到 mongo-query-to-sql.ts 文件中
  //
  ///**
  // * 查询文档
  // */
  ///**
  // * 打印查询信息(包含EXPLAIN结果)
  // */
  //private async logQueryInfo(
  //    sqlQuery: SQLiteQueryWithParams,
  //    preparedQuery: ExtendedPreparedQuery<RxDocType>
  //) {
  //    try {
  //        const database = await this.internals.databasePromise;
  //        const explainQuery = {
  //            query: 'EXPLAIN QUERY PLAN ' + sqlQuery.query,
  //            params: sqlQuery.params,
  //            context: sqlQuery.context
  //        };
  //
  //        const explainResult = await this.all(database, explainQuery);
  //
  //        let output = `\nSQLite Query Plan for table ${this.tableName}:\n`;
  //        output += `SQL: ${sqlQuery.query}\n`;
  //        output += `Params: ${JSON.stringify(sqlQuery.params)}\n`;
  //        output += 'EXPLAIN RESULT:\n';
  //        explainResult.forEach(row => {
  //            output += `${row.detail}\n`;
  //        });
  //        output += `Non-implemented Operators: ${JSON.stringify(preparedQuery.nonImplementedOperators || [])}\n`;
  //
  //        console.log(output);
  //    } catch (err) {
  //        console.error('Failed to explain query:', err);
  //    }
  //}
  ;
  _proto2.query = async function query(preparedQuery) {
    var database = await this.internals.databasePromise;

    // 将Mango查询转换为SQLite JSON查询
    var converter = (0, _mongoQueryToSql.createMongoQuerySQLConverter)({
      regexSupport: this.settings.regexSupport || false,
      query: preparedQuery,
      tableName: this.tableName,
      primaryPath: this.primaryPath,
      arrayFields: this.arrayFields
    });

    // 使用转换器进行查询转换
    var sqlQuery = converter.mangoQueryToSQLiteJSONQuery();

    //console.log("search query is " + JSON.stringify(preparedQuery, null, 2));
    //this.logQueryInfo(sqlQuery, preparedQuery);

    // 应用查询修改器（如果有）
    var finalQuery = this.settings.queryModifier ? this.settings.queryModifier(sqlQuery, preparedQuery) : sqlQuery;

    // 执行查询
    var result = await this.all(database, finalQuery);

    // 解析结果
    var documents = result.map(row => {
      return JSON.parse((0, _sqliteJsonHelpers.getDataFromResultRow)(row));
    });

    // 检查是否有不支持的操作符
    if (converter.hasUnSpoortedOperators) {
      var queryMatcher = (0, _index.getQueryMatcher)(this.schema, preparedQuery.query);
      return {
        documents: documents.filter(doc => queryMatcher(doc))
      };
    }
    return {
      documents
    };
  }

  /**
   * 计数查询
   */;
  _proto2.count = async function count(preparedQuery) {
    var database = await this.internals.databasePromise;

    // 如果有不支持的操作符，使用内存中过滤
    if (preparedQuery.nonImplementedOperators && preparedQuery.nonImplementedOperators.length > 0) {
      var results = await this.query(preparedQuery);
      return {
        count: results.documents.length,
        mode: 'slow'
      };
    }

    // 创建一个新的转换器实例，并配置正则表达式支持
    var converter = (0, _mongoQueryToSql.createMongoQuerySQLConverter)({
      regexSupport: this.settings.regexSupport || false,
      query: preparedQuery,
      tableName: this.tableName,
      primaryPath: this.primaryPath,
      arrayFields: this.arrayFields
    });

    // 使用转换器进行查询转换
    var sqlQuery = converter.mangoQueryToSQLiteJSONQuery();
    // 修改查询以使用COUNT
    var countQuery = sqlQuery.query.replace(/SELECT id, data FROM/i, 'SELECT COUNT(*) as count FROM');

    // 移除ORDER BY子句（对COUNT没有影响）
    var queryWithoutOrder = countQuery.replace(/ORDER BY.*?(LIMIT|$)/i, '$1');

    // 执行查询
    var result = await this.all(database, {
      query: queryWithoutOrder,
      params: sqlQuery.params,
      context: sqlQuery.context
    });
    return {
      count: result[0].count,
      mode: 'fast'
    };
  }

  /**
   * 根据ID查找文档
   */;
  _proto2.findDocumentsById = async function findDocumentsById(ids, withDeleted) {
    var database = await this.internals.databasePromise;
    if (this.closed) {
      throw new Error('SQLiteJSON.findDocumentsById() already closed ' + this.tableName);
    }
    if (ids.length === 0) {
      return [];
    }

    // 构建查询
    var placeholders = ids.map(() => '?').join(', ');
    var query = "SELECT data FROM \"" + this.tableName + "\" WHERE id IN (" + placeholders + ")";
    if (!withDeleted) {
      query += " AND deleted = 0";
    }
    var result = await this.all(database, {
      query,
      params: ids,
      context: {
        method: 'findDocumentsById',
        data: ids
      }
    });

    // 解析结果
    return result.map(row => {
      try {
        var rowData = (0, _sqliteJsonHelpers.getDataFromResultRow)(row);
        if (typeof rowData === 'string') {
          return JSON.parse(rowData);
        } else if (typeof rowData === 'object' && rowData !== null) {
          return rowData;
        }
        return null;
      } catch (err) {
        console.error('Failed to parse document data:', err);
        return null;
      }
    }).filter(doc => doc !== null);
  }

  /**
   * 变更流
   */;
  _proto2.changeStream = function changeStream() {
    return this.changes$.asObservable();
  }

  /**
   * 清理已删除的文档
   */;
  _proto2.cleanup = async function cleanup(minimumDeletedTime) {
    await (0, _index.promiseWait)(0);
    var database = await this.internals.databasePromise;

    // 清理已删除的文档
    var minTimestamp = new Date().getTime() - minimumDeletedTime;
    await this.run(database, {
      query: "\n                    DELETE FROM\n                        \"" + this.tableName + "\"\n                    WHERE\n                        deleted = 1\n                        AND\n                        lastWriteTime < ?\n                ",
      params: [minTimestamp],
      context: {
        method: 'cleanup',
        data: minimumDeletedTime
      }
    });
    return true;
  }

  /**
   * 获取附件数据
   * 当前实现不支持附件
   */;
  _proto2.getAttachmentData = async function getAttachmentData(_documentId, _attachmentId) {
    throw (0, _index.newRxError)('SNJ1', {
      args: {
        documentId: _documentId,
        attachmentId: _attachmentId
      }
    });
  }

  /**
   * 删除集合
   */;
  _proto2.remove = async function remove() {
    if (this.closed) {
      throw new Error('closed already');
    }
    var database = await this.internals.databasePromise;
    var promises = [this.run(database, {
      query: "DROP TABLE IF EXISTS \"" + this.tableName + "\"",
      params: [],
      context: {
        method: 'remove',
        data: this.tableName
      }
    })];
    await Promise.all(promises);
    return this.close();
  }

  /**
   * 关闭存储实例
   */;
  _proto2.close = async function close() {
    var queue = _sqliteJsonHelpers.TX_QUEUE_BY_DATABASE.get(await this.internals.databasePromise);
    if (queue) {
      await queue;
    }
    if (this.closed) {
      return this.closed;
    }
    this.closed = (async () => {
      await (0, _rxjs.firstValueFrom)(this.openWriteCount$.pipe((0, _rxjs.filter)(v => v === 0)));
      var database = await this.internals.databasePromise;

      // 首先获取事务，确保当前运行的操作已完成
      await (0, _sqliteJsonHelpers.sqliteTransaction)(database, this.sqliteBasics, () => {
        return Promise.resolve('COMMIT');
      }).catch(() => {});
      this.changes$.complete();
      await (0, _sqliteJsonHelpers.closeDatabaseConnection)(this.internalDatabaseName, this.storage.settings.sqliteBasics);
    })();
    return this.closed;
  };
  return RxStorageInstanceSQLiteJSON;
}();
/**
 * 创建SQLite JSON存储实例
 */
async function createSQLiteJSONStorageInstance(storage, params, settings) {
  var sqliteBasics = settings.sqliteBasics;
  var tableName = params.collectionName + '-' + params.schema.version;

  // 不支持附件
  if (params.schema.attachments) {
    throw (0, _index.newRxError)('SNJ1', {
      args: {
        message: 'SQLite JSON storage does not support attachments'
      }
    });
  }
  var internals = {};
  var useDatabaseName = (settings.databaseNamePrefix ? settings.databaseNamePrefix : '') + '_' + params.databaseName;

  // 获取数据库连接
  internals.databasePromise = (0, _sqliteJsonHelpers.getDatabaseConnection)(storage.settings.sqliteBasics, useDatabaseName).then(async database => {
    await (0, _sqliteJsonHelpers.sqliteTransaction)(database, sqliteBasics, async () => {
      // 创建表
      var tableQuery = "\n                CREATE TABLE IF NOT EXISTS \"" + tableName + "\"(\n                    id TEXT NOT NULL PRIMARY KEY UNIQUE,\n                    revision TEXT,\n                    deleted BOOLEAN NOT NULL CHECK (deleted IN (0, 1)),\n                    lastWriteTime INTEGER NOT NULL,\n                    data json\n                );\n                ";
      await sqliteBasics.run(database, {
        query: tableQuery,
        params: [],
        context: {
          method: 'createSQLiteJSONStorageInstance create tables',
          data: params.databaseName
        }
      });

      // 确定要索引的字段
      var indexedFields = params.schema.indexes ?? [];
      for (var field of indexedFields) {
        var indexQuery = (0, _sqliteJsonHelpers.createJsonIndexSQL)(tableName, field);
        await sqliteBasics.run(database, indexQuery);
      }
      return 'COMMIT';
    }, {
      indexCreation: true,
      databaseName: params.databaseName,
      collectionName: params.collectionName
    });
    return database;
  });

  // 创建存储实例
  var instance = new RxStorageInstanceSQLiteJSON(storage, params.databaseName, params.collectionName, params.schema, internals, params.options || {}, settings, tableName, params.devMode, useDatabaseName);

  // 添加多实例支持
  (0, _index.addRxStorageMultiInstanceSupport)(_sqliteJsonHelpers.RX_STORAGE_NAME_SQLITE_JSON, params, instance);
  return instance;
}
//# sourceMappingURL=index.js.map