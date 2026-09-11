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
     * <br>正值表示自当前时刻起剩余的有效时长，0表示刚到期/未知，负值表示已过期(绝对值为已超时的时长)
     */
    public final long pTtl;

    public DataPack(DataCore<Data> dataCore, Object provider, long pTtl) {
        this.dataCore = dataCore;
        this.provider = provider;
        this.pTtl = pTtl;
    }

    public Data getData() {
        if (norm()) {
            return dataCore.data;
        }
        throw dataCore.exception;
    }

    public boolean norm() {
        return dataCore.norm;
    }

}
