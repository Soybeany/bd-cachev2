package com.soybeany.cache.v2.contract;

import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataPack;

import java.util.Map;

/**
 * 可用于日志输出
 *
 * @author Soybeany
 * @date 2020/10/19
 */
public interface ILogger {

    /**
     * 获取数据时的回调
     */
    <Param, Data> void onGetData(DataContext<Param> context, DataPack<Data> pack, boolean needStore);

    /**
     * 缓存数据时的回调
     */
    <Param, Data> void onCacheData(DataContext<Param> context, DataPack<Data> pack);

    /**
     * 批量缓存数据时的回调
     */
    <Param, Data> void onBatchCacheData(DataContext.Core contextCore, Map<DataContext.Param<Param>, DataPack<Data>> dataPacks);

    /**
     * 失效缓存时的回调
     */
    <Param> void onInvalidCache(DataContext<Param> context, int... storageIndexes);

    /**
     * 失效全部缓存时的回调
     */
    void onInvalidAllCache(DataContext.Core core, int... storageIndexes);

    /**
     * 移除缓存时的回调
     */
    <Param> void onRemoveCache(DataContext<Param> context, int... storageIndexes);

    /**
     * 为已过期缓存续期时的回调
     */
    <Param> void onRenewExpiredCache(DataContext<Param> context, Object provider);

    /**
     * 清除缓存时的回调
     */
    void onClearCache(DataContext.Core core, int... storageIndexes);

    /**
     * 检测缓存是否存在的回调
     */
    <Param> void onContainCache(DataContext<Param> context, boolean exist);

    /**
     * 检查缓存是否存在更新的回调
     */
    <Param> void onCheckCache(DataContext<Param> context, boolean needUpdate);

}
