package com.soybeany.cache.v2.contract;


import com.soybeany.cache.v2.exception.BdCacheException;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataPack;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * 缓存存储器
 *
 * @author Soybeany
 * @date 2020/1/19
 */
public interface ICacheStorage<Param, Data> {

    // ********************配置类********************

    /**
     * 该存储器的描述
     *
     * @return 具体的描述文本
     */
    String desc();

    /**
     * 初始化时的回调
     *
     * @param contextCore 上下文核心
     */
    @SuppressWarnings("unused")
    default void onInit(DataContext.Core contextCore) {
        // 子类实现
    }

    // ********************操作回调类********************

    /**
     * 获取缓存
     *
     * @param context 上下文，含有当前环境的一些信息
     * @return 数据
     */
    DataPack<Data> onGetCache(DataContext<Param> context) throws NoCacheException;

    /**
     * 缓存数据
     *
     * @param context  上下文，含有当前环境的一些信息
     * @param dataPack 待缓存的数据
     * @return 返回至上一级的数据
     */
    DataPack<Data> onCacheData(DataContext<Param> context, DataPack<Data> dataPack);

    /**
     * 批量缓存数据
     *
     * @param contextCore 上下文核心，含有当前环境的一些固定信息
     * @param dataPacks   待缓存的数据
     * @return 返回至上一级的数据
     */
    default Map<DataContext.Param<Param>, DataPack<Data>> onBatchCacheData(DataContext.Core contextCore, Map<DataContext.Param<Param>, DataPack<Data>> dataPacks) {
        Map<DataContext.Param<Param>, DataPack<Data>> result = new HashMap<>();
        dataPacks.forEach((k, v) -> result.put(k, onCacheData(new DataContext<>(contextCore, k), v)));
        return result;
    }

    /**
     * 失效指定的缓存
     *
     * @param context 上下文，含有当前环境的一些信息
     */
    void onInvalidCache(DataContext<Param> context);

    /**
     * 失效全部缓存
     */
    default void onInvalidAllCache(DataContext.Core contextCore) {
        throw new BdCacheException("不支持此功能");
    }

    /**
     * 移除指定的缓存
     *
     * @param context 上下文，含有当前环境的一些信息
     */
    void onRemoveCache(DataContext<Param> context);

    /**
     * 清除全部缓存
     */
    void onClearCache(DataContext.Core contextCore);

    /**
     * 获取下次检查的时间戳
     */
    default long getNextCheckStamp(DataContext<Param> context) {
        throw new BdCacheException("不支持此功能");
    }

    /**
     * 设置下次检查的时间戳
     */
    default void setNextCheckStamp(DataContext<Param> context, long stamp) {
        throw new BdCacheException("不支持此功能");
    }

    // ***********************主动调用类****************************

    /**
     * 是否允许在数据源出现异常时，使用上一次已失效的缓存数据，使用异常的生存时间
     */
    void enableRenewExpiredCache(boolean enable);

    /**
     * 当前缓存的数据条数
     *
     * @return 数目
     */
    int cachedDataCount(DataContext.Core contextCore);

    // ***********************内部类****************************

    interface ILockSupport<L, BL> {
        L onTryLock(String key, long lockWaitTime);

        void onUnlock(L lock);

        BL onBatchLock(Collection<String> keys, long lockWaitTime);

        void onBatchUnlock(BL lock);
    }

}
