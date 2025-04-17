package com.soybeany.cache.v2.contract.frame;


import com.soybeany.cache.v2.exception.BdCacheException;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.model.DataParam;

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
     * @param context 上下文
     */
    @SuppressWarnings("unused")
    default void onInit(DataContext context) {
        // 子类实现
    }

    // ********************操作回调类********************

    /**
     * 获取缓存
     *
     * @param param 入参信息
     * @return 数据
     */
    DataPack<Data> onGetCache(DataParam<Param> param) throws NoCacheException;

    /**
     * 缓存数据
     *
     * @param param    入参信息
     * @param dataPack 待缓存的数据
     * @return 返回至上一级的数据
     */
    DataPack<Data> onCacheData(DataParam<Param> param, DataPack<Data> dataPack);

    /**
     * 批量缓存数据
     *
     * @param dataPacks 待缓存的数据
     * @return 返回至上一级的数据
     */
    default Map<DataParam<Param>, DataPack<Data>> onBatchCacheData(Map<DataParam<Param>, DataPack<Data>> dataPacks) {
        Map<DataParam<Param>, DataPack<Data>> result = new HashMap<>();
        dataPacks.forEach((k, v) -> result.put(k, onCacheData(k, v)));
        return result;
    }

    /**
     * 失效指定的缓存
     */
    void onInvalidCache(DataParam<Param> param);

    /**
     * 失效全部缓存
     */
    default void onInvalidAllCache() {
        throw new BdCacheException("不支持此功能");
    }

    /**
     * 移除指定的缓存
     */
    void onRemoveCache(DataParam<Param> param);

    /**
     * 清除全部缓存
     */
    void onClearCache();

    // ***********************主动调用类****************************

    /**
     * 获取下次检查的时间戳
     */
    default long getNextCheckStamp(DataParam<Param> param) {
        throw new BdCacheException("不支持此功能");
    }

    /**
     * 设置下次检查的时间戳
     */
    default void setNextCheckStamp(DataParam<Param> param, long stamp) {
        throw new BdCacheException("不支持此功能");
    }

    /**
     * 是否允许在数据源出现异常时，使用上一次已失效的缓存数据，使用异常的生存时间
     */
    void enableRenewExpiredCache(boolean enable);

    /**
     * 当前缓存的数据条数
     *
     * @return 数目
     */
    int cachedDataCount();
}
