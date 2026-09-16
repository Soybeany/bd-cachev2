package com.soybeany.cache.v2.contract.user;

import com.soybeany.cache.v2.model.DataCore;

/**
 * 数据获取器，封装了数据源访问逻辑(含异步/超时/异常包装)
 * <br>* 访问超时或数据源异常时，返回包含相应异常的DataCore，而不抛出异常
 * <br>* 调用方取得DataCore后自行组装DataPack，以自由指定有效期/数据来源/创建时间
 * <br>* Created by Soybeany on 2026/9/8.
 */
public interface IDataFetcher<Data> {

    /**
     * 以当前请求的param访问数据源获取数据
     */
    DataCore<Data> getData();

    /**
     * 本次取数的数据提供者(数据源，无数据源时为数据管理器)，供调用方组装DataPack时透传
     */
    Object getProvider();

}
