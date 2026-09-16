package com.soybeany.cache.v2.contract.user;

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
     * <br>* 返回的数据包会被写回各级缓存，实际生效的有效期取min(返回的pTtl，各级缓存配置的最大生存时间)
     *
     * @param param      请求参数
     * @param cachedPack 第一个缓存中已有的数据包，可能有效也可能已过期，可能为null(所有缓存级均无数据)；
     *                   pTtl大于0表示仍有效，不大于0表示已过期(负值的绝对值为已超时的时长)；
     *                   pCreateAt为数据首次写入缓存的时间戳，0表示未知(如旧版本写入的缓存数据)，可用于评估数据的陈旧程度
     * @param fetcher    数据获取器，封装了数据源访问逻辑；取回的DataCore由调用方自行组装为数据包，
     *                   数据来源可通过fetcher.getProvider()透传，创建时间由组装时的pCreateAt指定
     * @return 取得的数据包，须为非null且pTtl大于0，否则视为无效；
     *         pCreateAt为0时表示创建时间未知，将在落入缓存时补齐为首次写入的时间
     */
    DataPack<Data> onInvoke(DataParam<Param> param, DataPack<Data> cachedPack, IDataFetcher<Data> fetcher);

}
