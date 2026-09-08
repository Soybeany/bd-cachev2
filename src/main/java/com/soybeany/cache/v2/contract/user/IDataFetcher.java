package com.soybeany.cache.v2.contract.user;

import com.soybeany.cache.v2.model.DataPack;

/**
 * 数据获取器，封装了数据源访问逻辑(含异步/超时/异常包装)
 * <br>* 访问超时或数据源异常时，返回包含相应异常的DataPack，而不抛出异常
 * <br>* Created by Soybeany on 2026/9/8.
 */
public interface IDataFetcher<Data> {

    /**
     * 以当前请求的param访问数据源获取数据
     */
    DataPack<Data> getData();

}
