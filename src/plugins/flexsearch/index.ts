import type { RxPlugin } from '../../types/index';
import type { RxFlexSearchOptions } from './types';
import RxDBStorage from './rx-storage';
import { Index } from 'flexsearch'

/**
 * RxDB FlexSearch 插件
 * 为RxDB提供集成FlexSearch的全文搜索功能
 */
export const RxDBFlexSearchPlugin: RxPlugin = {
    name: 'flexsearch',
    rxdb: true,
    prototypes: {
        RxCollection: (proto: any) => {
            proto.addFlexSearch = function (options: RxFlexSearchOptions<any>) {
                return addFulltextSearch(options);
            };
        }
    }
};

/**
 * 为集合添加全文搜索功能
 * 
 * @param options FlexSearch配置选项
 * @returns 带有search方法的FlexSearch实例
 */
export async function addFulltextSearch<RxDocType>(
    options: RxFlexSearchOptions<RxDocType>
) {
    // 使用从flexsearch导入的Index类
    // 不需要动态导入
    // 获取RxDB数据库实例
    const database = options.collection.database;

    // 创建RxDBStorage实例
    const storage = new RxDBStorage({
        name: options.identifier,
        field: 'fulltext',
        db: database
    });

    // 初始化FlexSearch实例
    const flexSearch = new Index({
        tokenize: 'forward',
        optimize: true,
        cache: 100,
        ...options.indexOptions,
        store: true
    });

    // 将存储挂载到FlexSearch
    await storage.mount(flexSearch);

    // 订阅集合变更
    let initialized = false;

    // 为RxCollection创建索引
    async function indexDocument(doc: any) {
        if (doc.deleted) {
            // 如果文档被删除，从索引中移除
            await flexSearch.remove(doc.primary);
        } else {
            // 生成搜索文本
            const searchText = options.docToString(doc);

            // 将文档添加到索引 - FlexSearch.add(id, text)
            // 第三个参数在我们使用的接口中不存在，应该使用store参数设置
            await flexSearch.add(doc.primary, searchText);
        }
    }

    // 初始索引所有现有文档
    async function indexAllDocuments() {
        if (initialized) return;

        const batchSize = options.batchSize || 100;
        let lastId = null;
        let hasMore = true;

        // 分批处理文档以避免内存问题
        while (hasMore) {
            let query = options.collection.find();

            if (lastId) {
                // 使用主键字段而不是_id
                query = query.where(options.collection.schema.primaryPath).gt(lastId);
            }

            const docs = await query.limit(batchSize).exec();

            if (docs.length === 0) {
                hasMore = false;
                break;
            }

            // 索引这批文档
            for (const doc of docs) {
                await indexDocument(doc);
                // 获取文档主键值
                lastId = doc.primary;
            }

            // 提交更改
            await flexSearch.commit();
        }

        initialized = true;
    }

    // 监听集合变更
    const subscription = options.collection.$.subscribe(async (changeEvent) => {
        if (!initialized) return;

        if (changeEvent.operation === 'INSERT' || changeEvent.operation === 'UPDATE') {
            await indexDocument(changeEvent.documentData);
        } else if (changeEvent.operation === 'DELETE') {
            await flexSearch.remove(changeEvent.documentId);
        }

        // 提交更改到存储
        await flexSearch.commit();
    });

    // 立即或延迟初始化
    if (options.initialization !== 'lazy') {
        await indexAllDocuments();
    }

    // 返回用于搜索的接口
    return {
        /**
         * 搜索文档
         * @param searchString 搜索字符串
         * @param searchOptions 搜索选项
         * @returns 匹配的文档数组
         */
        find: async (searchString: string, searchOptions: any = {}) => {
            if (!initialized && options.initialization === 'lazy') {
                await indexAllDocuments();
            }

            // 执行搜索，结果可能是不同的格式
            const results = await flexSearch.search(searchString, searchOptions) as any;
            
            // 处理不同格式的搜索结果
            if (Array.isArray(results)) {
                // 如果结果是ID数组
                return await Promise.all(
                    results.map(async (id: string) => {
                        const doc = await options.collection.findOne(id).exec();
                        return doc ? doc.toJSON() : null;
                    })
                );
            } else if (typeof results === 'object') {
                // 如果结果是包含文档的对象
                return Object.values(results).map((doc: any) => doc);
            }
            
            // 默认返回空数组
            return [];
        },

        /**
         * 搜索文档并返回详细结果（包含分数）
         * @param searchString 搜索字符串
         * @param searchOptions 搜索选项
         * @returns 带有分数的搜索结果数组
         */
        search: async (searchString: string, searchOptions: any = {}) => {
            if (!initialized && options.initialization === 'lazy') {
                await indexAllDocuments();
            }

            // 执行搜索，需要处理不同格式的结果
            const results = await flexSearch.search(searchString, {
                ...searchOptions,
                // 得分模式
                return: "score"
            }) as any;
            
            // 如果结果是对象格式 {id: score, ...}
            if (results && typeof results === 'object' && !Array.isArray(results)) {
                const resultArray = [];
                
                for (const id in results) {
                    if (Object.prototype.hasOwnProperty.call(results, id)) {
                        const doc = await options.collection.findOne(id).exec();
                        if (doc) {
                            resultArray.push({
                                id: id,
                                score: results[id],
                                document: doc.toJSON()
                            });
                        }
                    }
                }
                
                return resultArray;
            }
            
            // 默认返回空数组
            return [];
        },

        /**
         * 销毁搜索实例并清理资源
         */
        destroy: async () => {
            subscription.unsubscribe();
            await storage.destroy();
        }
    };
}

export * from './types';
