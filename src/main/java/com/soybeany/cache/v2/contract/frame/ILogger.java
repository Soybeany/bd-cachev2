package com.soybeany.cache.v2.contract.frame;

import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.model.DataParam;

import java.util.Map;

/**
 * 可用于日志输出
 *
 * @author Soybeany
 * @date 2020/10/19
 */
public interface ILogger {

    ILogger SKIP = new ILogger() {
    };

    /**
     * 初始化
     */
    default void onInit(DataContext context) {
    }

    /**
     * 获取数据时的回调
     */
    default <Param, Data> void onGetData(DataParam<Param> param, DataPack<Data> pack, boolean needStore) {
    }

    /**
     * 缓存数据时的回调
     */
    default <Param, Data> void onCacheData(DataParam<Param> param, DataPack<Data> pack) {
    }

    /**
     * 批量缓存数据时的回调
     */
    default <Param, Data> void onBatchCacheData(Map<DataParam<Param>, DataPack<Data>> dataPacks) {
    }

    /**
     * 失效缓存时的回调
     */
    default <Param> void onInvalidCache(DataParam<Param> param, int... storageIndexes) {
    }

    /**
     * 失效全部缓存时的回调
     */
    default void onInvalidAllCache(int... storageIndexes) {
    }

    /**
     * 移除缓存时的回调
     */
    default <Param> void onRemoveCache(DataParam<Param> param, int... storageIndexes) {
    }

    /**
     * 为已过期缓存续期时的回调
     */
    default <Param> void onRenewExpiredCache(DataParam<Param> param, Object provider) {
    }

    /**
     * 清除缓存时的回调
     */
    default void onClearCache(int... storageIndexes) {
    }

    /**
     * 检测缓存是否存在的回调
     */
    default <Param> void onContainCache(DataParam<Param> param, boolean exist) {
    }

    /**
     * 检查缓存是否存在更新的回调
     */
    default <Param> void onCheckCache(DataParam<Param> param, boolean needUpdate) {
    }

    /**
     * 锁异常时的回调
     *
     * @param key 加/解锁用的key（可空，表示全局锁）
     */
    default void onLockException(String key, Exception ex) {
    }
}
