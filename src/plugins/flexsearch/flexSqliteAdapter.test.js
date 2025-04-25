import { Database } from 'bun:sqlite';
import { getRxStorageSQLiteJSON } from '../storage-sqlite-json'
import { RxDBStorage as FlexStorage } from './rx-storage'
import { Index, Document, Charset } from 'flexsearch';
import RedisDatabase from "flexsearch/db/mongodb";
import { createRxDatabase } from '../../index'
import { test, expect, describe } from "bun:test";
import { addRxPlugin } from '../../plugin'
import { RxDBQueryBuilderPlugin } from '../query-builder';

addRxPlugin(RxDBQueryBuilderPlugin);

// 实现BunSqliteBasic，与example.ts中的实现相同
class BunSqliteBasic {
  constructor() {
    this.journalMode = '';
  }

  async open(name) {
    const res = new Database(name);
    return res;
  }

  all(db, queryWithParams) {
    return new Promise((resolve, reject) => {
      try {
        const stmt = db.prepare(queryWithParams.query);
        const rows = stmt.all(...queryWithParams.params);
        resolve(rows);
      } catch (error) {
        reject(error);
      }
    });
  }

  run(db, queryWithParams) {
    return new Promise((resolve, reject) => {
      try {
        const stmt = db.prepare(queryWithParams.query);
        stmt.run(...queryWithParams.params);
        resolve();
      } catch (error) {
        reject(error);
      }
    });
  }

  setPragma(db, key, value) {
    return new Promise((resolve, reject) => {
      try {
        // 对于空值或特殊值，使用不同的处理方式
        if (value === '' || value === undefined || value === null) {
          // 如果值为空，只执行PRAGMA key
          db.exec(`PRAGMA ${key};`);
        } else if (value.includes('"') || value.includes("'") || value.includes(';')) {
          // 如果值包含引号或分号，使用参数化查询
          const stmt = db.prepare(`PRAGMA ${key} = ?`);
          stmt.run(value);
        } else {
          // 普通情况
          db.exec(`PRAGMA ${key} = ${value}`);
        }
        resolve();
      } catch (error) {
        console.error(`Error setting PRAGMA ${key} = ${value}:`, error);
        // 对于PRAGMA错误，我们不想中断整个流程，所以仍然resolve
        resolve();
      }
    });
  }

  close(db) {
    return new Promise((resolve, reject) => {
      try {
        db.close();
        resolve();
      } catch (error) {
        reject(error);
      }
    });
  }
}

// 测试数据
const testData = [{
  "tconst": "tt0000001",
  "titleType": "short",
  "primaryTitle": "Carmencita",
  "originalTitle": "Carmencita",
  "isAdult": 0,
  "startYear": "1894",
  "endYear": "",
  "runtimeMinutes": "1",
  "genres": [
    "Documentary",
    "Short"
  ]
}, {
  "tconst": "tt0000002",
  "titleType": "short",
  "primaryTitle": "Le clown et ses chiens",
  "originalTitle": "Le clown et ses chiens",
  "isAdult": 0,
  "startYear": "1892",
  "endYear": "",
  "runtimeMinutes": "5",
  "genres": [
    "Animation",
    "Short"
  ]
}];

// 生成一个唯一的数据库名称，避免冲突
const dbName = 'flex-search-' + Date.now();

// 确保删除之前的数据库文件
try {
  const fs = require('fs');
  if (fs.existsSync(dbName + '.sqlite')) {
    fs.unlinkSync(dbName + '.sqlite');
  }
  if (fs.existsSync('_' + dbName)) {
    fs.rmdirSync('_' + dbName, { recursive: true });
  }
} catch (e) {
  console.warn('清理数据库文件失败:', e);
}

const sqliteBasic = new BunSqliteBasic();
const rxdb = await createRxDatabase({
  name: dbName,
  storage: getRxStorageSQLiteJSON({
    sqliteBasics: sqliteBasic
  })
})
const useDb = rxdb;
const Databse = FlexStorage

// 创建测试套件
describe("FlexSqliteAdapter", () => {
  // 创建一个BunSqliteBasic实例

  test("应该正确创建实例并支持基本索引操作", async () => {

    const db = new Databse("test-basic", {
      db: useDb
    })
    // 验证实例属性和方法
    expect(typeof db.mount).toBe("function");
    expect(typeof db.close).toBe("function");
    expect(db).toHaveProperty("id");
    expect(db).toHaveProperty("field");
    expect(db).toHaveProperty("db");

    // 创建一个简单索引
    let index = new Index({
      tokenize: "strict"
    });

    // 挂载数据库到索引
    await index.mount(db);
    expect(index.db).toBe(db);
    await index.destroy();
    expect(index.db).toBe(db);
    // 挂载数据库到索引
    await db.mount(index);
    expect(index.db).toBe(db);

    // 测试数据
    const data = [
      'cats abcd efgh ijkl mnop qrst uvwx cute',
      'cats abcd efgh ijkl mnop dogs cute',
      'cats abcd efgh ijkl mnop cute',
      'cats abcd efgh ijkl cute',
      'cats abcd efgh cute',
      'cats abcd cute',
      'cats cute'
    ];

    // 添加数据到索引
    for (let i = 0; i < data.length; i++) {
      index.add(i, data[i]);
    }

    expect(index.reg.size).toBe(7);
    expect(index.map.size).not.toBe(0);

    await index.commit();

    expect(index.reg.size).toBe(0);
    expect(index.map.size).toBe(0);

    let result = await index.search("cats cute");
    expect(result).toEqual([6, 5, 4, 3, 2, 1, 0]);

    result = await index.search("cute cats");
    expect(result).toEqual([6, 5, 4, 3, 2, 1, 0]);

    result = await index.search("cute");
    expect(result).toEqual([6, 5, 4, 3, 2, 1, 0]);

    result = await index.search("cute dogs cats");
    expect(result).toEqual([1]);

    result = await index.search("cute dogs cats", { suggest: true });
    expect(result).toEqual([1, 6, 5, 4, 3, 2, 0]);

    result = await index.search("undefined cute undefined dogs undefined cats undefined", { suggest: true });
    expect(result).toEqual([1, 6, 5, 4, 3, 2, 0]);

    result = await index.search("cute cat");
    expect(result.length).toBe(0);

    await index.destroy();
    await index.db.close();
  });

  test("应该正确创建实例并支持上下文搜索", async () => {
    const db = new Databse("test-context", {
      db: useDb
    })


    // 验证实例属性和方法
    expect(typeof db.mount).toBe("function");
    expect(typeof db.close).toBe("function");
    expect(db).toHaveProperty("id");
    expect(db).toHaveProperty("field");
    expect(db).toHaveProperty("db");

    // 创建一个简单索引
    let index = new Index({
      tokenize: "strict",
      //context: true
    });

    // 挂载数据库到索引
    await index.mount(db);
    expect(index.db).toBe(db);
    await index.clear();

    // 测试数据
    const data = [
      'cats abcd efgh ijkl mnop qrst uvwx cute',
      'cats abcd efgh ijkl mnop dogs cute',
      'cats abcd efgh ijkl mnop cute',
      'cats abcd efgh ijkl cute',
      'cats abcd efgh cute',
      'cats abcd cute',
      'cats cute'
    ];

    // 添加数据到索引
    for (let i = 0; i < data.length; i++) {
      index.add(i, data[i]);
    }

    expect(index.reg.size).toBe(7);
    expect(index.map.size).not.toBe(0);

    await index.commit();

    expect(index.reg.size).toBe(0);
    expect(index.map.size).toBe(0);

    let result = await index.search("cats cute");
    expect(result).toEqual([6, 5, 4, 3, 2, 1, 0]);

    result = await index.search("cute cats");
    expect(result).toEqual([6, 5, 4, 3, 2, 1, 0]);

    result = await index.search("cute dogs cats");
    expect(result).toEqual([1]);

    result = await index.search("cute");
    expect(result).toEqual([6, 5, 4, 3, 2, 1, 0]);

    result = await index.search("undefined cute undefined dogs undefined cats undefined", { suggest: true });
    expect(result).toEqual([1, 6, 5, 4, 3, 2, 0]);

    result = await index.search("cute cat");
    expect(result.length).toBe(0);

    await index.destroy();
    await index.db.close();
  });

  test("应该支持文档索引", async () => {
    // 创建DB实例


    const db = new Databse("test-document", {
      db: useDb
    })
    // 创建文档索引
    const document = new Document({
      encoder: Charset.LatinBalance,
      document: {
        id: "tconst",
        store: true,
        index: [{
          field: "primaryTitle",
          tokenize: "forward"
        }, {
          field: "originalTitle",
          tokenize: "forward"
        }],
        tag: [{
          field: "startYear"
        }, {
          field: "genres"
        }]
      }
    });

    // 挂载数据库到索引
    await document.mount(db);
    expect(document.index.get("primaryTitle").db).toBeInstanceOf(db.constructor);
    expect(document.index.get("originalTitle").db).toBeInstanceOf(db.constructor);
    expect(document.index.get("startYear").db).toBeInstanceOf(db.constructor);
    expect(document.index.get("genres").db).toBeInstanceOf(db.constructor);

    // 添加测试数据
    for (let i = 0; i < testData.length; i++) {
      document.add(testData[i]);
    }

    expect(document.index.get("primaryTitle").reg.size).toEqual(2);
    expect(document.index.get("primaryTitle").map.size).toEqual(25);
    expect(document.index.get("originalTitle").reg.size).toEqual(2);
    expect(document.index.get("originalTitle").map.size).toEqual(25);
    // 标签伪索引（仅持久化）
    expect(document.index.get("startYear").reg.size).toBe(2);
    expect(document.index.get("startYear").map.size).toBe(0);
    expect(document.index.get("genres").reg.size).toBe(2);
    expect(document.index.get("genres").map.size).toBe(0);
    expect(document.reg.size).toBe(2);
    expect(document.store.size).toBe(2);
    expect(document.tag.size).toBe(2);
    expect(document.tag.get("startYear").size).toBe(2);
    expect(document.tag.get("genres").size).toBe(3);

    // 批量提交更改
    await document.commit();

    expect(document.index.get("primaryTitle").reg.size).toBe(0);
    expect(document.index.get("primaryTitle").map.size).toBe(0);
    expect(document.index.get("originalTitle").reg.size).toBe(0);
    expect(document.index.get("originalTitle").map.size).toBe(0);
    expect(document.index.get("startYear").reg.size).toBe(0);
    expect(document.index.get("startYear").map.size).toBe(0);
    expect(document.index.get("genres").reg.size).toBe(0);
    expect(document.index.get("genres").map.size).toBe(0);
    expect(document.reg.size).toBe(0);
    expect(document.store.size).toBe(0);
    expect(document.tag.size).toBe(2);
    expect(document.tag.get("startYear").size).toBe(0);
    expect(document.tag.get("genres").size).toBe(0);

    expect(await document.contain(testData[0]["tconst"])).toBe(true);

    let result = await document.search({
      query: "carmen"
    });

    expect(result).toEqual([
      { field: 'primaryTitle', result: [testData[0]["tconst"]] },
      { field: 'originalTitle', result: [testData[0]["tconst"]] }
    ]);

    result = await document.search({
      query: "karmen",
      tag: {
        "startYear": "1894",
        "genres": [
          "Documentary",
          "Short"
        ]
      },
    });

    expect(result).toEqual([
      { field: 'primaryTitle', result: [testData[0]["tconst"]] },
      { field: 'originalTitle', result: [testData[0]["tconst"]] }
    ]);

    result = await document.search({
      query: "karmen",
      tag: {
        "startYear": "1894",
        "genres": [
          "Documentary",
          "Short"
        ]
      },
      enrich: true
    });

    expect(result).toEqual([{
      field: "primaryTitle",
      result: [{
        id: testData[0]["tconst"],
        doc: testData[0],
      }]
    }, {
      field: "originalTitle",
      result: [{
        id: testData[0]["tconst"],
        doc: testData[0],
      }]
    }]);

    result = await document.search({
      query: "karmen",
      tag: {
        "startYear": "1894",
        "genres": [
          "Documentary",
          "Short"
        ]
      },
      suggest: true,
      enrich: true
    });

    expect(result).toEqual([
      {
        field: 'primaryTitle', result: [{
          id: testData[0]["tconst"],
          doc: testData[0],
        }]
      },
      {
        field: 'originalTitle', result: [{
          id: testData[0]["tconst"],
          doc: testData[0],
        }]
      }
    ]);

    result = await document.search({
      query: "karmen or clown or nothing",
      suggest: true,
      enrich: true,
      merge: true
    });

    // 检查结果中是否包含特定项
    const containsItem1 = result.some((item) =>
      item.id === 'tt0000001' &&
      JSON.stringify(item.doc) === JSON.stringify(testData[0]) &&
      Array.isArray(item.field) &&
      item.field.includes('primaryTitle') &&
      item.field.includes('originalTitle')
    );

    const containsItem2 = result.some((item) =>
      item.id === 'tt0000002' &&
      JSON.stringify(item.doc) === JSON.stringify(testData[1]) &&
      Array.isArray(item.field) &&
      item.field.includes('primaryTitle') &&
      item.field.includes('originalTitle')
    );

    expect(containsItem1).toBe(true);
    expect(containsItem2).toBe(true);

    for (const index of document.index.values()) {
      index.destroy();
      index.db.close();
    }
  });

  test("应该支持结果高亮", async () => {
    // 测试数据
    const data = [{
      "id": 1,
      "title": "Carmencita"
    }, {
      "id": 2,
      "title": "Le clown et ses chiens"
    }];
    const db = new Databse("test-hilight", {
      db: useDb,
      type: "integer"
    })


    // 创建文档索引
    const index = new Document({
      cache: true,
      document: {
        store: true,
        index: [{
          field: "title",
          tokenize: "forward",
          encoder: Charset.LatinBalance,
        }]
      }
    });

    // 挂载数据库到索引
    await index.mount(db);

    // 添加测试数据
    for (let i = 0; i < data.length; i++) {
      index.add(data[i]);
    }

    await index.commit();

    // 执行查询
    let result = await index.searchCache({
      query: "karmen or clown or not found",
      suggest: true,
      // 设置enrich为true（必需）
      enrich: true,
      // 高亮模板
      // $1是匹配部分的占位符
      highlight: "<b>$1</b>"
    });

    expect(result[0].result).toEqual([{
      id: 1,
      doc: data[0],
      highlight: '<b>Carmen</b>cita'
    }, {
      id: 2,
      doc: data[1],
      highlight: 'Le <b>clown</b> et ses chiens'
    }]);


    // 在缓存上执行查询
    result = await index.searchCache({
      query: "karmen or clown or not found",
      suggest: true,
      // 设置enrich为true（必需）
      enrich: true,
      // 高亮模板
      // $1是匹配部分的占位符
      highlight: "<b>$1</b>"
    });

    expect(result[0].result).toEqual([{
      id: 1,
      doc: data[0],
      highlight: '<b>Carmen</b>cita'
    }, {
      id: 2,
      doc: data[1],
      highlight: 'Le <b>clown</b> et ses chiens'
    }]);


    // 使用pluck执行查询
    result = await index.search({
      query: "karmen or clown or not found",
      suggest: true,
      // 设置enrich为true（必需）
      enrich: true,
      pluck: "title",
      // 高亮模板
      // $1是匹配部分的占位符
      highlight: "<b>$1</b>"
    });

    expect(result).toEqual([{
      id: 1,
      doc: data[0],
      highlight: '<b>Carmen</b>cita'
    }, {
      id: 2,
      doc: data[1],
      highlight: 'Le <b>clown</b> et ses chiens'
    }]);

  });
});
