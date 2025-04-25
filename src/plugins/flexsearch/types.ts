import type { RxCollection, RxDocumentData } from '../../types/index';
import type { RxPipeline } from '../pipeline/rx-pipeline';

export interface RxFlexSearchOptions<RxDocType> {
    /**
     * 唯一标识符，用于存储元数据和在重启/重新加载时继续索引。
     */
    identifier: string;
    
    /**
     * 基于其文档进行搜索的源集合
     */
    collection: RxCollection<RxDocType>;
    
    /**
     * 将文档数据转换为可搜索字符串的函数
     * 可以通过返回文档的单个字符串属性，
     * 或者连接和转换多个字段，例如：
     * doc => doc.firstName + ' ' + doc.lastName
     */
    docToString: (doc: RxDocType) => string;
    
    /**
     * (可选) 一次索引的文档数量
     */
    batchSize?: number;
    
    /**
     * (可选)
     * lazy: 在第一次搜索查询时初始化内存中的全文索引
     * instant: 直接初始化索引，这样第一次查询时索引已经存在
     * 默认: 'instant'
     */
    initialization?: 'lazy' | 'instant';
    
    /**
     * (可选) FlexSearch索引选项
     * @link https://github.com/nextapps-de/flexsearch#index-options
     */
    indexOptions?: any;
}

/**
 * 存储在索引中的文档项
 */
export interface IndexDocItem<RxDocType> {
    /**
     * 文档的主键
     */
    id: string;
    
    /**
     * 用于搜索的文本内容
     */
    content: string;
    
    /**
     * 源文档的副本
     */
    document: RxDocumentData<RxDocType>;
}

/**
 * 全文搜索索引的元数据
 */
export interface FlexSearchIndexMeta {
    /**
     * 上次索引的时间戳
     */
    lastIndexed: number;
    
    /**
     * 索引文档的数量
     */
    documentCount: number;
}

/**
 * 搜索结果项
 */
export interface SearchResult<RxDocType> {
    /**
     * 文档的主键
     */
    id: string;
    
    /**
     * 搜索相关性分数
     */
    score: number;
    
    /**
     * 源文档
     */
    document: RxDocType;
}
