/**
 * This file contains the RxStorage implementation for SharedWorker
 */
import type { RxStorage } from '../../types';
import { RxStorageSharedWorkerSettings } from './shared-worker-types';
/**
 * Default settings for the shared worker storage
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
 * Creates a RxStorage that uses a SharedWorker
 */
export declare function getRxStorageSharedWorker(settings: RxStorageSharedWorkerSettings): RxStorage<RxStorageDefaultSettings, RxStorageDefaultCheckpoint>;
