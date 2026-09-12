import assert from 'assert';
import {
    fillWithDefaultSettings,
    prepareQuery,
    normalizeMangoQuery,
    randomToken,
    newRxError
} from '../../plugins/core/index.mjs';
import { getRxStorageSQLiteJSON, sqliteTransaction, TX_QUEUE_BY_DATABASE } from '../../plugins/storage-sqlite-json/index.mjs';
import { getSQLiteBasicsNodeNative } from '../../plugins/storage-sqlite/index.mjs';
import type { SQLiteQueryWithParams } from '../../plugins/storage-sqlite/index.mjs';
import { isNode } from '../../plugins/test-utils/index.mjs';
import config from './config.ts';

type Deferred<T> = {
    promise: Promise<T>;
    resolve: (value: T) => void;
};
function deferred<T>(): Deferred<T> {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>(res => {
        resolve = res;
    });
    return { promise, resolve };
}

const schema = {
    version: 0,
    primaryKey: 'id',
    type: 'object',
    properties: {
        id: { type: 'string', maxLength: 100 },
        value: { type: 'number' }
    },
    required: ['id', 'value']
} as const;

type TestInstance = any;

function createInstance(
    storage: any,
    databaseName: string,
    collectionName: string
): Promise<TestInstance> {
    return storage.createStorageInstance({
        databaseName,
        collectionName,
        schema: fillWithDefaultSettings(schema),
        options: {},
        databaseInstanceToken: randomToken(10),
        multiInstance: false,
        devMode: false
    });
}

function doc(id: string, value: number): any {
    return {
        id,
        value,
        _deleted: false,
        _rev: '1-' + id,
        _meta: { lwt: Date.now() + value },
        _attachments: {}
    };
}

describe('rx-storage-sqlite-json-lifecycle.test.ts', function () {
    this.timeout(60000);

    if (!isNode || config.storage.name !== 'sqlite-json') {
        return;
    }

    it('rolls back partial writes and recovers the queue after a write failure', async () => {
        const DatabaseSync = (await import('node:sqlite')).DatabaseSync;
        const original = getSQLiteBasicsNodeNative(DatabaseSync);
        let fail = true;
        const failureGate = deferred<void>();
        const basics = Object.assign({}, original, {
            run: async (database: any, query: SQLiteQueryWithParams) => {
                if (fail && query.query.includes('INSERT INTO "docs-0"') && query.params[0] === 'second') {
                    fail = false;
                    await failureGate.promise;
                    throw newRxError('SNH', { args: { reason: 'forced insert failure' } });
                }
                return original.run(database, query);
            }
        });
        const storage = getRxStorageSQLiteJSON({ sqliteBasics: basics, databaseNamePrefix: './test_tmp/' });
        const dbName = 'sqlite-json-lifecycle-' + randomToken(10);
        const instance = await createInstance(storage, dbName, 'docs');
        const events: any[] = [];
        const sub = instance.changeStream().subscribe((event: any) => events.push(event));
        try {
            const failedWrite = instance.bulkWrite([
                { document: doc('first', 1) },
                { document: doc('second', 2) }
            ], 'failed-write');
            failureGate.resolve(undefined);
            await assert.rejects(failedWrite);
            assert.deepStrictEqual(await instance.findDocumentsById(['first', 'second'], false), []);
            assert.strictEqual(events.length, 0);
            assert.strictEqual(instance.openWriteCount$.getValue(), 0);
            await instance.bulkWrite([{ document: doc('after', 3) }], 'recovery-write');
            assert.strictEqual((await instance.findDocumentsById(['after'], false)).length, 1);
        } finally {
            failureGate.resolve(undefined);
            sub.unsubscribe();
            await instance.remove();
        }
    });

    it('waits for an admitted write before external read and close', async () => {
        const DatabaseSync = (await import('node:sqlite')).DatabaseSync;
        const original = getSQLiteBasicsNodeNative(DatabaseSync);
        const insertGate = deferred<void>();
        const entered = deferred<void>();
        const statements: string[] = [];
        let closeCalls = 0;
        const basics = Object.assign({}, original, {
            all: (database: any, query: SQLiteQueryWithParams) => {
                statements.push(query.query);
                return original.all(database, query);
            },
            close: (database: any) => {
                closeCalls++;
                return original.close(database);
            },
            run: async (database: any, query: SQLiteQueryWithParams) => {
                statements.push(query.query);
                if (query.query.includes('INSERT INTO "docs-0"')) {
                    const result = await original.run(database, query);
                    entered.resolve(undefined);
                    await insertGate.promise;
                    return result;
                }
                return original.run(database, query);
            }
        });
        const storage = getRxStorageSQLiteJSON({ sqliteBasics: basics, databaseNamePrefix: './test_tmp/' });
        const instance = await createInstance(storage, 'sqlite-json-close-' + randomToken(10), 'docs');
        const events: any[] = [];
        const sub = instance.changeStream().subscribe((event: any) => events.push(event));
        try {
            const write = instance.bulkWrite([{ document: doc('paused', 1) }], 'paused-write');
            const writeFailure = write.catch((error: any) => {
                entered.resolve(undefined);
                throw error;
            });
            await entered.promise;
            statements.length = 0;
            const read = instance.findDocumentsById(['paused'], false);
            const close = instance.close();
            insertGate.resolve(undefined);
            await writeFailure;
            await close;
            assert.strictEqual((await read).length, 1);
            assert.strictEqual(events.length, 1);
            assert.strictEqual(statements[0], 'COMMIT;');
            assert.ok(statements[1].startsWith('SELECT data'));
            await instance.close();
            assert.strictEqual(closeCalls, 1);
            await assert.rejects(instance.findDocumentsById(['paused'], false));
        } finally {
            insertGate.resolve(undefined);
            sub.unsubscribe();
            await instance.close();
        }
    });

    it('keeps a sibling instance usable after the first instance closes', async () => {
        const DatabaseSync = (await import('node:sqlite')).DatabaseSync;
        const original = getSQLiteBasicsNodeNative(DatabaseSync);
        let closeCalls = 0;
        const basics = Object.assign({}, original, {
            close: (database: any) => {
                closeCalls++;
                return original.close(database);
            }
        });
        const storage = getRxStorageSQLiteJSON({
            sqliteBasics: basics,
            databaseNamePrefix: './test_tmp/'
        });
        const dbName = 'sqlite-json-shared-' + randomToken(10);
        const first = await createInstance(storage, dbName, 'first');
        const second = await createInstance(storage, dbName, 'second');
        try {
            await first.close();
            assert.strictEqual(closeCalls, 0);
            await second.bulkWrite([{ document: doc('still-open', 1) }], 'sibling-write');
            assert.strictEqual((await second.findDocumentsById(['still-open'], false)).length, 1);
        } finally {
            await second.remove();
        }
        assert.strictEqual(closeCalls, 1);
    });

    it('drains cleanup before removing tables and rejects new operations', async () => {
        const DatabaseSync = (await import('node:sqlite')).DatabaseSync;
        const original = getSQLiteBasicsNodeNative(DatabaseSync);
        const entered = deferred<void>();
        const release = deferred<void>();
        const statements: string[] = [];
        const basics = Object.assign({}, original, {
            all: async (database: any, query: SQLiteQueryWithParams) => {
                const result = await original.all(database, query);
                if (query.context.method === 'cleanup_select') {
                    entered.resolve(undefined);
                    await release.promise;
                }
                return result;
            },
            run: (database: any, query: SQLiteQueryWithParams) => {
                statements.push(query.query);
                return original.run(database, query);
            }
        });
        const storage = getRxStorageSQLiteJSON({ sqliteBasics: basics, databaseNamePrefix: './test_tmp/' });
        const instance = await createInstance(storage, 'sqlite-json-remove-' + randomToken(10), 'docs');
        try {
            const cleanup = instance.cleanup(0);
            await Promise.race([entered.promise, cleanup]);
            const removed = instance.remove();
            await assert.rejects(instance.bulkWrite([{ document: doc('late', 1) }], 'late'));
            release.resolve(undefined);
            await Promise.all([cleanup, removed]);
            const deletion = statements.findIndex(sql => sql.startsWith('DELETE FROM'));
            const drop = statements.findIndex(sql => sql.startsWith('DROP TABLE'));
            assert.ok(deletion >= 0 && drop > deletion);
            assert.strictEqual(statements[deletion + 1], 'COMMIT;');
        } finally {
            release.resolve(undefined);
            await instance.close();
        }
    });

    it('releases failed initialization and permits a new acquisition', async () => {
        const DatabaseSync = (await import('node:sqlite')).DatabaseSync;
        const original = getSQLiteBasicsNodeNative(DatabaseSync);
        const failure = newRxError('SNH', { args: { reason: 'forced initialization failure' } });
        let fail = true;
        let opens = 0;
        let closes = 0;
        const basics = Object.assign({}, original, {
            open: (databaseName: string) => {
                opens++;
                return original.open(databaseName);
            },
            close: (database: any) => {
                closes++;
                return original.close(database);
            },
            run: (database: any, query: SQLiteQueryWithParams) => {
                if (fail && query.query.includes('CREATE TABLE')) {
                    fail = false;
                    return Promise.reject(failure);
                }
                return original.run(database, query);
            }
        });
        const storage = getRxStorageSQLiteJSON({ sqliteBasics: basics, databaseNamePrefix: './test_tmp/' });
        const name = 'sqlite-json-init-' + randomToken(10);
        await assert.rejects(createInstance(storage, name, 'docs'), err => err === failure);
        assert.strictEqual(closes, 1);
        const instance = await createInstance(storage, name, 'docs');
        try {
            assert.strictEqual(opens, 2);
            await instance.bulkWrite([{ document: doc('retry', 1) }], 'retry');
        } finally {
            await instance.remove();
        }
        assert.strictEqual(closes, 2);
    });

    it('evicts a rejected open from the connection cache', async () => {
        const DatabaseSync = (await import('node:sqlite')).DatabaseSync;
        const original = getSQLiteBasicsNodeNative(DatabaseSync);
        const failure = newRxError('SNH', { args: { reason: 'forced open failure' } });
        let fail = true;
        let opens = 0;
        const basics = Object.assign({}, original, {
            open: (databaseName: string) => {
                opens++;
                if (fail) {
                    fail = false;
                    return Promise.reject(failure);
                }
                return original.open(databaseName);
            }
        });
        const storage = getRxStorageSQLiteJSON({ sqliteBasics: basics, databaseNamePrefix: './test_tmp/' });
        const name = 'sqlite-json-open-' + randomToken(10);
        await assert.rejects(createInstance(storage, name, 'docs'), err => err === failure);
        const instance = await createInstance(storage, name, 'docs');
        try {
            assert.strictEqual(opens, 2);
        } finally {
            await instance.remove();
        }
    });

    it('preserves the write error on failed rollback and still physically closes', async () => {
        const DatabaseSync = (await import('node:sqlite')).DatabaseSync;
        const original = getSQLiteBasicsNodeNative(DatabaseSync);
        const failure = newRxError('SNH', { args: { reason: 'forced write failure' } });
        const rollbackFailure = newRxError('SNH', { args: { reason: 'forced rollback failure' } });
        let closes = 0;
        const basics = Object.assign({}, original, {
            close: (database: any) => {
                closes++;
                return original.close(database);
            },
            run: (database: any, query: SQLiteQueryWithParams) => {
                if (query.query.includes('INSERT INTO')) {
                    return Promise.reject(failure);
                }
                if (query.query === 'ROLLBACK;') {
                    return Promise.reject(rollbackFailure);
                }
                return original.run(database, query);
            }
        });
        const storage = getRxStorageSQLiteJSON({ sqliteBasics: basics, databaseNamePrefix: './test_tmp/' });
        const instance = await createInstance(storage, 'sqlite-json-broken-' + randomToken(10), 'docs');
        try {
            await assert.rejects(instance.bulkWrite([{ document: doc('broken', 1) }], 'broken'), err => err === failure);
            await assert.rejects(instance.findDocumentsById(['broken'], false), err => err === rollbackFailure);
            assert.strictEqual(instance.openWriteCount$.getValue(), 0);
        } finally {
            await instance.close();
        }
        assert.strictEqual(closes, 1);
    });

    it('preserves the DROP error when connection release also fails', async () => {
        const DatabaseSync = (await import('node:sqlite')).DatabaseSync;
        const original = getSQLiteBasicsNodeNative(DatabaseSync);
        const dropFailure = newRxError('SNH', { args: { reason: 'forced drop failure' } });
        const closeFailure = newRxError('SNH', { args: { reason: 'forced close failure' } });
        let closeCalls = 0;
        const basics = Object.assign({}, original, {
            close: async (database: any) => {
                closeCalls++;
                await original.close(database);
                throw closeFailure;
            },
            run: (database: any, query: SQLiteQueryWithParams) => {
                if (query.query.startsWith('DROP TABLE')) {
                    return Promise.reject(dropFailure);
                }
                return original.run(database, query);
            }
        });
        const storage = getRxStorageSQLiteJSON({ sqliteBasics: basics, databaseNamePrefix: './test_tmp/' });
        const instance = await createInstance(storage, 'sqlite-json-remove-errors-' + randomToken(10), 'docs');
        await assert.rejects(instance.remove(), err => err === dropFailure);
        assert.strictEqual(closeCalls, 1);
    });

    it('keeps legacy transactions and explicit rollback on the owner queue', async () => {
        const DatabaseSync = (await import('node:sqlite')).DatabaseSync;
        const basics = getSQLiteBasicsNodeNative(DatabaseSync);
        const storage = getRxStorageSQLiteJSON({ sqliteBasics: basics, databaseNamePrefix: './test_tmp/' });
        const instance = await createInstance(storage, 'sqlite-json-legacy-' + randomToken(10), 'docs');
        try {
            await instance.bulkWrite([{ document: doc('kept', 1) }], 'legacy-data');
            const database = await instance.getSQLiteDatabase();
            const transaction = sqliteTransaction(database, basics, async () => {
                await basics.run(database, {
                    query: 'DELETE FROM "docs-0"', params: [], context: { method: 'test', data: null }
                });
                return 'ROLLBACK';
            });
            const read = instance.findDocumentsById(['kept'], false);
            assert.ok(TX_QUEUE_BY_DATABASE.get(database));
            await transaction;
            assert.strictEqual((await read).length, 1);
        } finally {
            await instance.remove();
        }
    });

    it('counts the final paginated query in fast and slow modes', async () => {
        const DatabaseSync = (await import('node:sqlite')).DatabaseSync;
        const storage = getRxStorageSQLiteJSON({
            sqliteBasics: getSQLiteBasicsNodeNative(DatabaseSync),
            databaseNamePrefix: './test_tmp/'
        });
        const instance = await createInstance(storage, 'sqlite-json-paginated-count-' + randomToken(10), 'docs');
        try {
            await instance.bulkWrite([
                { document: doc('one', 1) },
                { document: doc('two', 2) },
                { document: doc('three', 3) }
            ], 'count-data');
            const querySchema = fillWithDefaultSettings(schema);
            const fastPrepared = prepareQuery(querySchema, normalizeMangoQuery(querySchema, {
                selector: { value: { $gte: 1 } },
                sort: [{ id: 'asc' }],
                skip: 1,
                limit: 1
            }));
            const slowPrepared = prepareQuery(querySchema, normalizeMangoQuery(querySchema, {
                selector: { value: { $mod: [1, 0] } } as any,
                sort: [{ id: 'asc' }],
                skip: 1,
                limit: 1
            }));

            const fastQuery = await instance.query(fastPrepared);
            const fastCount = await instance.count(fastPrepared);
            assert.strictEqual(fastCount.count, fastQuery.documents.length);
            assert.strictEqual(fastCount.mode, 'fast');

            const slowQuery = await instance.query(slowPrepared);
            const slowCount = await instance.count(slowPrepared);
            assert.strictEqual(slowCount.count, slowQuery.documents.length);
            assert.strictEqual(slowCount.mode, 'slow');
        } finally {
            await instance.remove();
        }
    });

    it('applies a narrowing query modifier in unsupported count fallback', async () => {
        const DatabaseSync = (await import('node:sqlite')).DatabaseSync;
        let modifierCalls = 0;
        const storage = getRxStorageSQLiteJSON({
            sqliteBasics: getSQLiteBasicsNodeNative(DatabaseSync),
            databaseNamePrefix: './test_tmp/',
            queryModifier: query => {
                modifierCalls++;
                return {
                    ...query,
                    query: `SELECT * FROM (${query.query}) WHERE id != 'two'`
                };
            }
        });
        const instance = await createInstance(storage, 'sqlite-json-count-' + randomToken(10), 'docs');
        try {
            await instance.bulkWrite([
                { document: doc('one', 2) },
                { document: doc('two', 2) },
                { document: doc('three', 3) }
            ], 'count-data');
            const querySchema = fillWithDefaultSettings(schema);
            const prepared = prepareQuery(querySchema, normalizeMangoQuery(querySchema, {
                selector: { value: { $mod: [2, 0] } } as any,
                sort: [{ id: 'asc' }],
                skip: 0,
                limit: 10
            }));
            const queried = await instance.query(prepared);
            const counted = await instance.count(prepared);
            assert.strictEqual(queried.documents.length, 1);
            assert.strictEqual(counted.count, queried.documents.length);
            assert.ok(modifierCalls >= 2);
        } finally {
            await instance.remove();
        }
    });

    it('forwards the final fast count query and supports positional rows', async () => {
        const DatabaseSync = (await import('node:sqlite')).DatabaseSync;
        const original = getSQLiteBasicsNodeNative(DatabaseSync);
        let countQuery: SQLiteQueryWithParams | undefined;
        const basics = Object.assign({}, original, {
            all: async (database: any, query: SQLiteQueryWithParams): Promise<any[]> => {
                const result = await original.all(database, query);
                if (query.query.startsWith('SELECT COUNT(*) AS count FROM')) {
                    countQuery = query;
                    return [[(result[0] as any).count]];
                }
                return result;
            }
        });
        const storage = getRxStorageSQLiteJSON({
            sqliteBasics: basics,
            databaseNamePrefix: './test_tmp/',
            queryModifier: query => ({
                ...query,
                query: `SELECT * FROM (${query.query}) WHERE id != 'two'`,
                context: { method: 'modified-query', data: query.context }
            })
        });
        const instance = await createInstance(storage, 'sqlite-json-array-count-' + randomToken(10), 'docs');
        try {
            await instance.bulkWrite([
                { document: doc('one', 1) },
                { document: doc('two', 2) },
                { document: doc('three', 3) }
            ], 'count-data');
            const querySchema = fillWithDefaultSettings(schema);
            const prepared = prepareQuery(querySchema, normalizeMangoQuery(querySchema, {
                selector: { value: { $gte: 1 } },
                sort: [{ id: 'asc' }],
                skip: 0,
                limit: 2
            }));
            const preparedBefore = JSON.stringify(prepared);
            const counted = await instance.count(prepared);
            assert.strictEqual(counted.count, 2);
            assert.strictEqual(counted.mode, 'fast');
            assert.ok(countQuery);
            assert.ok(countQuery.query.includes('WHERE id != \'two\''));
            assert.deepStrictEqual(countQuery.params, [1]);
            assert.strictEqual(countQuery.context.method, 'modified-query');
            assert.strictEqual(JSON.stringify(prepared), preparedBefore);
        } finally {
            await instance.remove();
        }
    });
});
