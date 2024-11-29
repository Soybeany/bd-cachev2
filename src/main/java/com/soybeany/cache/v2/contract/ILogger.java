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
public interface ILogger<Param, Data> {

    /**
     * 获取数据时的回调
     */
    void onGetData(DataContext<Param> context, DataPack<Data> pack);

    /**
     * 缓存数据时的回调
     */
    void onCacheData(DataContext<Param> context, DataPack<Data> pack);

    /**
     * 批量缓存数据时的回调
     */
    void onBatchCacheData(DataContext.Core<Param, Data> contextCore, Map<DataContext.Param<Param>, DataPack<Data>> dataPacks);

    /**
     * 移除缓存时的回调
     */
    void onInvalidCache(DataContext<Param> context, int... storageIndexes);

    /**
     * 移除缓存时的回调
     */
    void onRemoveCache(DataContext<Param> context, int... storageIndexes);

    /**
     * 为已过期缓存续期时的回调
     */
    void onRenewExpiredCache(DataContext<Param> context, Object provider);

    /**
     * 清除缓存时的回调
     */
    void onClearCache(String dataDesc, int... storageIndexes);

}
