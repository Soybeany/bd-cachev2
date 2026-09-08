package com.soybeany.cache.v2.contract.user;

import com.soybeany.cache.v2.model.DataCore;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.model.DataParam;

/**
 * 缓存未命中(全部缓存均失效)时的处理器，用于自定义回源取数逻辑
 * <br>* Created by Soybeany on 2026/9/8.
 */
public interface ICacheMissHandler<Param, Data> {

    /**
     * 处理缓存未命中
     * <br>* 该方法在获取锁内执行，需自行保证线程安全，且不宜执行耗时过长的逻辑
     * <br>* 返回的数据包会被写回各级缓存，有效期由各级缓存的有效期配置决定
     *
     * @param param       请求参数
     * @param invalidCore 已过期的旧缓存数据，可能为null(所有缓存级均无旧数据)
     * @param fetcher     数据获取器，封装了数据源访问逻辑
     * @return 取得的数据包
     */
    DataPack<Data> onInvoke(DataParam<Param> param, DataCore<Data> invalidCore, IDataFetcher<Data> fetcher);

}
