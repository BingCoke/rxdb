/**
 * RxDB存储适配器，用于FlexSearch
 * 实现FlexSearch的存储接口，使FlexSearch可以在RxDB中存储索引数据
 */

import type { RxDatabase, RxCollection, RxDocument } from '../../types/index.ts';
import { clone } from '../utils/index.ts';

/**
 * RxDB存储配置接口
 */
export interface RxDBStorageOptions {
    /**
     * 存储的名称（用于集合命名）
     */
    name?: string;

    /**
     * 用于全文搜索的字段名
     */
    field?: string;

    /**
     * 存储的类型
     */
    type?: string;

    /**
     * 要使用的RxDatabase实例
     */
    db: RxDatabase;
}

/**
 * FlexSearch索引文档架构
 */
interface FlexSearchIndexItem {
    id: string;
    type: 'map' | 'ctx' | 'reg' | 'tag' | 'cfg';
    data: any;
}

/**
 * RxDB存储适配器，用于FlexSearch
 */
export class RxDBStorage {
    /**
     * 存储ID
     */
    id: string;

    /**
     * 字段名
     */
    field: string;

    /**
     * 存储类型
     */
    type: string;

    /**
     * RxDatabase实例
     */
    db: RxDatabase;

    /**
     * 存储集合
     */
    collection!: RxCollection<FlexSearchIndexItem>;

    /**
     * 是否支持标签搜索
     */
    support_tag_search: boolean = false;

    /**
     * 是否启用快速更新
     */
    fastupdate: boolean = false;

    /**
     * @param {string|RxDBStorageOptions} name 存储名称或配置对象
     * @param {RxDBStorageOptions} config 配置对象
     */
    constructor(name: string | RxDBStorageOptions, config: RxDBStorageOptions = {} as RxDBStorageOptions) {
        if (typeof name === 'object') {
            config = name;
            name = config.name || '';
        }

        if (!name) {
            console.info('默认存储空间被使用，因为没有传递名称。');
        }

        // 存储标识符
        this.id = 'flexsearch' + (name ? ':' + this.sanitize(name as string) : '');

        // 字段名
        this.field = config.field ? this.sanitize(config.field) : '';

        // 存储类型
        this.type = config.type || '';

        // RxDatabase实例
        this.db = config.db;
    }

    /**
     * 净化字符串（只保留字母、数字、下划线和连字符）
     */
    private sanitize(str: string): string {
        return str.toLowerCase().replace(/[^a-z0-9_\-]/g, '');
    }

    /**
     * 为不同类型的数据构建ID
     */
    private getDocId(type: string, id: string): string {
        return type + '_' + id;
    }

    /**
     * 创建集合架构
     */
    private getCollectionSchema() {
        const schema = {
            version: 0,
            primaryKey: 'id',
            type: 'object',
            properties: {
                id: {
                    type: 'string',
                    maxLength: 150
                },
                type: {
                    type: 'string',
                    enum: ['map', 'ctx', 'reg', 'tag', 'cfg']
                },
                data: {
                    type: ['object', 'array', 'string', 'number', 'boolean', 'null']
                }
            },
            required: ['id', 'type', 'data'],
            indexes: [
                'type',
                ['type', 'id']
            ]
        };

        return schema;
    }

    /**
     * 将存储挂载到FlexSearch索引
     */
    async mount(flexsearch: any): Promise<any> {
        if (flexsearch.index) {
            return flexsearch.mount(this);
        }

        flexsearch.db = this;
        return this.open();
    }

    /**
     * 打开存储连接
     */
    async open(): Promise<any> {
        // 创建单个集合用于所有类型的数据
        // 基于name和field构建集合名称
        const name = this.id;
        const collectionName = 'flexsearch' + (name ? '_' + name : '') + (this.field ? '_' + this.field : '');
        
        const collections = await this.db.addCollections({
            [collectionName]: {
                schema: this.getCollectionSchema()

            }
        });

        this.collection = collections[collectionName]

        return this;
    }

    /**
     * 关闭存储连接
     */
    close(): void {
        // RxDB会自动处理连接的关闭
        // this.collection.close()
        // @ts-ignore
        this.collection = null;
    }

    /**
     * 销毁存储（删除集合）
     */
    async destroy(): Promise<void> {
        if (this.collection) {
            // 使用remove方法删除集合
            await this.collection.remove();
            // @ts-ignore
            this.collection = undefined;
        }
    }

    /**
     * 清除所有数据
     */
    async clear(): Promise<void> {
        // 从集合中删除所有文档
        const allDocs = await this.collection.find().exec();
        for (const doc of allDocs) {
            await doc.remove();
        }
    }

    /**
     * 检查特定ID是否存在
     */
    async has(id: string | number): Promise<boolean> {
        const docId = this.getDocId('reg', id.toString());
        const doc = await this.collection.findOne({
            selector: {
                id: docId
            }
        }).exec();

        return !!doc;
    }

    /**
     * 获取term的搜索结果
     */
    async get(
        key: string,
        ctx?: string,
        limit: number = 0,
        offset: number = 0,
        resolve: boolean = true,
        enrich: boolean = false
    ): Promise<any> {
        const type = ctx ? 'ctx' : 'map';
        const docId = this.getDocId(type, ctx ? ctx + ':' + key : key);

        const doc = await this.collection.findOne({
            selector: {
                id: docId
            }
        }).exec();

        if (!doc || !doc.data || !doc.data.length) {
            return [];
        }

        let result = [];

        if (resolve) {
            if (!limit && !offset && doc.data.length === 1) {
                return doc.data[0];
            }

            for (let i = 0, arr; i < doc.data.length; i++) {
                if ((arr = doc.data[i]) && arr.length) {
                    if (offset >= arr.length) {
                        offset -= arr.length;
                        continue;
                    }

                    const end = limit
                        ? offset + Math.min(arr.length - offset, limit)
                        : arr.length;

                    for (let j = offset; j < end; j++) {
                        result.push(arr[j]);
                    }

                    offset = 0;

                    if (result.length === limit) {
                        break;
                    }
                }
            }

            return enrich ? await this.enrich(result) : result;
        } else {
            return doc.data;
        }
    }

    /**
     * 使用文档数据丰富搜索结果，并处理高亮
     * 
     * @param ids ID列表或对象列表
     * @param options 可选的配置（比如高亮模板）
     */
    async enrich(ids: any[]): Promise<any[]> {
        if (typeof ids !== 'object') {
            ids = [ids];
        }

        const results = [];

        for (const id of ids) {
            // 创建结果对象
            let result: any = {};
            
            // 确保有id
            const idValue = typeof id === 'object' && id !== null ? id.id : id;
            result.id = idValue;
            
            // 查询文档数据
            const docId = this.getDocId('reg', idValue.toString());
            const doc = await this.collection.findOne({
                selector: {
                    id: docId
                }
            }).exec();
            
            // 设置文档数据
            result.doc = doc ? (typeof doc.data === 'string' ? JSON.parse(doc.data) : doc.data) : null;
            
            results.push(result);
        }

        return results;
    }

    /**
     * 根据标签获取文档
     */
    async tag(
        tag: string,
        limit: number = 0,
        offset: number = 0,
        enrich: boolean = false
    ): Promise<any[]> {
        const docId = this.getDocId('tag', tag);

        const doc = await this.collection.findOne({
            selector: {
                id: docId
            }
        }).exec();

        if (!doc || !doc.data || !doc.data.length || offset >= doc.data.length) {
            return [];
        }

        if (!limit && !offset) {
            return doc.data;
        }

        const result = doc.data.slice(offset, offset + limit);

        return enrich ? await this.enrich(result) : result;
    }

    /**
     * 删除特定ID或多个ID
     */
    async remove(ids: string | number | (string | number)[]): Promise<void> {
        if (typeof ids !== 'object') {
            ids = [ids];
        }

        // 转换为字符串数组
        const idStrings = (ids as (string | number)[]).map(id => id.toString());

        // 处理基本集合 (reg)
        for (const id of idStrings) {
            const docId = this.getDocId('reg', id);
            const doc = await this.collection.findOne({
                selector: {
                    id: docId
                }
            }).exec();

            if (doc) {
                await doc.remove();
            }
        }

        // 处理map文档
        const mapDocs = await this.collection.find({
            selector: {
                type: 'map'
            }
        }).exec();

        for (const doc of mapDocs) {
            let changed = false;

            for (let i = 0; i < doc.data.length; i++) {
                const arr = doc.data[i];

                if (arr && arr.length) {
                    for (const id of idStrings) {
                        const pos = arr.indexOf(id);

                        if (pos >= 0) {
                            changed = true;

                            if (arr.length > 1) {
                                arr.splice(pos, 1);
                            } else {
                                doc.data[i] = [];
                            }
                        }
                    }
                }
            }

            // 检查是否有内容
            let hasContent = false;

            for (const arr of doc.data) {
                if (arr && arr.length) {
                    hasContent = true;
                    break;
                }
            }

            if (!hasContent) {
                await doc.remove();
            } else if (changed) {
                // 更新文档数据
                await doc.patch({
                    data: doc.data
                });
            }
        }

        // 处理ctx文档
        const ctxDocs = await this.collection.find({
            selector: {
                type: 'ctx'
            }
        }).exec();

        for (const doc of ctxDocs) {
            let changed = false;

            for (let i = 0; i < doc.data.length; i++) {
                const arr = doc.data[i];

                if (arr && arr.length) {
                    for (const id of idStrings) {
                        const pos = arr.indexOf(id);

                        if (pos >= 0) {
                            changed = true;

                            if (arr.length > 1) {
                                arr.splice(pos, 1);
                            } else {
                                doc.data[i] = [];
                            }
                        }
                    }
                }
            }

            // 检查是否有内容
            let hasContent = false;

            for (const arr of doc.data) {
                if (arr && arr.length) {
                    hasContent = true;
                    break;
                }
            }

            if (!hasContent) {
                await doc.remove();
            } else if (changed) {
                // 更新文档数据
                await doc.patch({
                    data: doc.data
                });
            }
        }

        // 处理tag文档
        const tagDocs = await this.collection.find({
            selector: {
                type: 'tag'
            }
        }).exec();

        for (const doc of tagDocs) {
            let changed = false;

            for (const id of idStrings) {
                const pos = doc.data.indexOf(id);

                if (pos >= 0) {
                    changed = true;

                    if (doc.data.length > 1) {
                        doc.data.splice(pos, 1);
                    } else {
                        doc.data = [];
                    }
                }
            }

            if (!doc.data.length) {
                await doc.remove();
            } else if (changed) {
                // 更新文档数据
                await doc.patch({
                    data: doc.data
                });
            }
        }
    }

    /**
     * 将索引的所有更改提交到数据库
     */
    async commit(flexsearch: any, _replace: boolean, _append: boolean): Promise<void> {
        // 处理清理任务
        if (_replace) {
            await this.clear();
            // 任务队列中只有删除操作
            flexsearch.commit_task = [];
        } else {
            let tasks = flexsearch.commit_task;
            flexsearch.commit_task = [];

            for (let i = 0, task; i < tasks.length; i++) {
                task = tasks[i];

                // 任务队列中只有删除操作
                if (task.clear) {
                    await this.clear();
                    _replace = true;
                    break;
                } else {
                    tasks[i] = task.del;
                }
            }

            if (!_replace) {
                if (!_append) {
                    // 合并任务和注册表
                    if (flexsearch.reg && flexsearch.reg.size) {
                        const regArray = Array.from(flexsearch.reg.keys());
                        tasks = tasks.concat(regArray);
                    }
                }

                if (tasks.length) {
                    await this.remove(tasks);
                }
            }
        }

        if (!flexsearch.reg || !flexsearch.reg.size) {
            return;
        }

        // 处理map数据
        for (const [key, value] of flexsearch.map.entries()) {
            if (!value.length) {
                continue;
            }

            const docId = this.getDocId('map', key);

            if (_replace) {
                // 替换模式 - 直接插入
                // 更新或插入文档
                if (this.collection) {
                    // RxDB的集合方法，使用upsert替代atomicUpsert
                    await this.collection.upsert({
                        id: docId,
                        type: 'map',
                        data: value
                    });
                }
            } else {
                // 更新模式 - 合并现有数据
                const doc = await this.collection.findOne({
                    selector: {
                        id: docId
                    }
                }).exec();

                if (doc) {
                    let result = doc.data;
                    let changed = false;

                    const maxlen = Math.max(result.length, value.length);

                    for (let i = 0; i < maxlen; i++) {
                        const val = value[i];

                        if (val && val.length) {
                            const res = result[i];

                            if (res && res.length) {
                                for (let j = 0; j < val.length; j++) {
                                    res.push(val[j]);
                                }
                                changed = true;
                            } else {
                                result[i] = val;
                                changed = true;
                            }
                        }
                    }

                    if (changed) {
                        // 更新文档数据
                        await doc.patch({
                            data: result
                        });
                    }
                } else {
                    // 文档不存在，直接插入
                    // 更新或插入文档
                    if (this.collection) {
                        await this.collection.upsert({
                            id: docId,
                            type: 'map',
                            data: value
                        });
                    }
                }
            }
        }

        // 处理ctx数据
        for (const [ctxKey, ctxValue] of flexsearch.ctx.entries()) {
            for (const [key, value] of ctxValue.entries()) {
                if (!value.length) {
                    continue;
                }

                const rawId = ctxKey + ':' + key;
                const docId = this.getDocId('ctx', rawId);

                if (_replace) {
                    // 替换模式 - 直接插入
                    // 更新或插入文档
                    if (this.collection) {
                        await this.collection.upsert({
                            id: docId,
                            type: 'ctx',
                            data: value
                        });
                    }
                } else {
                    // 更新模式 - 合并现有数据
                    const doc = await this.collection.findOne({
                        selector: {
                            id: docId
                        }
                    }).exec();

                    if (doc) {
                        let result = doc.data;
                        let changed = false;

                        const maxlen = Math.max(result.length, value.length);

                        for (let i = 0; i < maxlen; i++) {
                            const val = value[i];

                            if (val && val.length) {
                                const res = result[i];

                                if (res && res.length) {
                                    for (let j = 0; j < val.length; j++) {
                                        res.push(val[j]);
                                    }
                                    changed = true;
                                } else {
                                    result[i] = val;
                                    changed = true;
                                }
                            }
                        }

                        if (changed) {
                            // 更新文档数据
                            await doc.patch({
                                data: result
                            });
                        }
                    } else {
                        // 文档不存在，直接插入
                        // 更新或插入文档
                        if (this.collection) {
                            await this.collection.upsert({
                                id: docId,
                                type: 'ctx',
                                data: value
                            });
                        }
                    }
                }
            }
        }

        // 处理reg数据（注册表）
        // 存储在注册表中
        if (flexsearch.store) {
            // 存储完整文档
            for (const [id, docData] of flexsearch.store.entries()) {
                const docId = this.getDocId('reg', id.toString());
                // 更新或插入文档
                if (this.collection) {
                    await this.collection.upsert({
                        id: docId,
                        type: 'reg',
                        data: typeof docData === 'object' ? docData : 1
                    });
                }
            }
        } else if (!flexsearch.bypass) {
            // 只存储ID
            for (const id of flexsearch.reg.keys()) {
                const docId = this.getDocId('reg', id.toString());
                // 更新或插入文档
                if (this.collection) {
                    await this.collection.upsert({
                        id: docId,
                        type: 'reg',
                        data: 1
                    });
                }
            }
        }

        // 处理tag数据
        if (flexsearch.tag) {
            for (const [tag, ids] of flexsearch.tag.entries()) {
                if (!ids.length) {
                    continue;
                }

                const docId = this.getDocId('tag', tag);
                const doc = await this.collection.findOne({
                    selector: {
                        id: docId
                    }
                }).exec();

                if (doc) {
                    // 合并现有标签
                    const result = doc.data.concat(ids);

                    // 更新文档数据
                    await doc.patch({
                        data: result
                    });
                } else {
                    // 创建新标签
                    // 更新或插入文档
                    if (this.collection) {
                        await this.collection.upsert({
                            id: docId,
                            type: 'tag',
                            data: ids
                        });
                    }
                }
            }
        }

        // 清理内存中的数据
        flexsearch.map.clear();
        flexsearch.ctx.clear();

        if (flexsearch.tag) {
            flexsearch.tag.clear();
        }

        if (flexsearch.store) {
            flexsearch.store.clear();
        }

        if (!flexsearch.document) {
            flexsearch.reg.clear();
        }
    }

    /**
     * 从搜索结果中提取扁平的结果数组
     * 用于处理searchCache和search(pluck:true)方法的返回格式
     */
    extractPluckResults(results: any): any {
        // 处理searchCache的结果，将其转换为扁平结构
        if (Array.isArray(results) && results.length > 0 && results[0].field && Array.isArray(results[0].result)) {
            return results[0].result;
        }
        return results;
    }

    /**
     * 在数据库端执行查询交集
     */
    search = null;

    /**
     * 提供关于存储的一些信息
     */
    async info() {
        // 提供有关存储的统计信息
        const result: { [key: string]: any } = {};
        const types = ['map', 'ctx', 'reg', 'tag', 'cfg'];

        if (this.collection) {
            for (const type of types) {
                const count = await this.collection.count({
                    selector: {
                        type: type as any
                    } as any
                }).exec();

                result[type] = { count };
            }
        }

        return result;
    }
}

/**
 * 导出RxDBStorage作为FlexSearch存储适配器
 */
export default RxDBStorage;
