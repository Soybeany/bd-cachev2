package com.soybeany.cache.v2.contract.user;

import com.soybeany.cache.v2.model.DataPack;

public interface ICacheChecker<Param, Data> {

    /**
     * 判断缓存是否需要更新
     *
     * @param param    请求参数
     * @param dataPack 上一次缓存的数据包
     * @return 是否需要更新
     */
    boolean needUpdate(Param param, DataPack<Data> dataPack);
}
