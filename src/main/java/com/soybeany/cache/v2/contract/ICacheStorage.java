package com.soybeany.cache.v2.contract;


import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataPack;

/**
 * 缓存存储器
 *
 * @author Soybeany
 * @date 2020/1/19
 */
public interface ICacheStorage<Param, Data> {

    /**
     * 默认的优先级
     */
    int ORDER_DEFAULT = 10;

    // ********************配置类********************

    /**
     * 指定优先级
     *
     * @return 优先级值，值越大，越先被使用
     */
    default int priority() {
        return ORDER_DEFAULT;
    }

    /**
     * 在获取数据时，是否需要双重检查
     */
    default boolean needDoubleCheck() {
        return true;
    }

    /**
     * 该存储器的描述
     *
     * @return 具体的描述文本
     */
    String desc();

    /**
     * 初始化时的回调
     *
     * @param storageId 数据存储的唯一id
     */
    default void onInit(String storageId) {
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
     * 移除指定的缓存
     *
     * @param context 上下文，含有当前环境的一些信息
     */
    void onRemoveCache(DataContext<Param> context);

    /**
     * 清除全部缓存
     */
    void onClearCache(String storageId);

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
    int cachedDataCount(String storageId);

}
