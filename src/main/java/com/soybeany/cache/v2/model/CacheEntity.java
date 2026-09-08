package com.soybeany.cache.v2.model;

/**
 * 缓存存储器内部使用的实体
 * <br>Created by Soybeany on 2020/11/25.
 */
public class CacheEntity<Data> {

    /**
     * 目标数据缓存
     */
    public final DataCore<Data> dataCore;

    /**
     * 该数据失效的时间戳(时间点)
     */
    public final long pExpireAt;

    /**
     * 该数据下次检查的时间戳(时间点)
     */
    public long pNextCheckAt;

    /**
     * @param pTtlMax 该级缓存允许的最大生存时间(时间段)
     */
    public static <Data> CacheEntity<Data> fromDataPack(DataPack<Data> dataPack, long curTimestamp, long pTtlMax) {
        // 实际使用min(缓存配置值，数据/异常剩余有效期)
        long pTtl = Math.min(dataPack.pTtl, pTtlMax);
        return new CacheEntity<>(dataPack.dataCore, curTimestamp + pTtl);
    }

    public static <Data> DataPack<Data> toDataPack(CacheEntity<Data> entity, Object provider, long curTimestamp) {
        return new DataPack<>(entity.dataCore, provider, entity.pExpireAt - curTimestamp);
    }

    public CacheEntity(DataCore<Data> dataCore, long pExpireAt) {
        this.dataCore = dataCore;
        this.pExpireAt = pExpireAt;
    }

    public boolean isExpired(long curTimestamp) {
        return curTimestamp > pExpireAt;
    }

}
