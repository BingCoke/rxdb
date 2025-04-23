/**
 * This file contains the RxStorage implementation for Worker
 */
import type { RxStorage } from '../../types';
import { RxStorageWorkerSettings } from './worker-types';
/**
 * Default settings for the worker storage
 */
export type RxStorageDefaultSettings = {
    /**
     * Any additional options
     */
    [key: string]: any;
};
/**
 * Default checkpoint type
 */
export type RxStorageDefaultCheckpoint = {
    /**
     * Any checkpoint data
     */
    [key: string]: any;
};
/**
 * Creates a RxStorage that uses a Worker
 */
export declare function getRxStorageWorker(settings: RxStorageWorkerSettings): RxStorage<RxStorageDefaultSettings, RxStorageDefaultCheckpoint>;
