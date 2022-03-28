package com.soybeany.cache.v2.model;

/**
 * 节点间用于通讯的对象
 * <br>Created by Soybeany on 2020/11/24.
 */
public class DataPack<Data> {

    /**
     * 目标数据缓存
     */
    public final DataCore<Data> dataCore;

    /**
     * 数据的提供者，即数据最近一次的提供者
     */
    public final Object provider;

    /**
     * 该数据的生存时间[Time To Live](时间段)
     */
    public final int pTtl;

    public DataPack(DataCore<Data> dataCore, Object provider, int pTtl) {
        this.dataCore = dataCore;
        this.provider = provider;
        this.pTtl = Math.max(pTtl, 0);
    }

    public Data getData() throws Exception {
        if (norm()) {
            return dataCore.data;
        }
        throw dataCore.exception;
    }

    public boolean norm() {
        return dataCore.norm;
    }

}
