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

    public static <Data> CacheEntity<Data> fromDataPack(DataPack<Data> dataPack, long curTimestamp, long pTtlMaxNorm, long pTtlMaxErr) {
        long pTtlMax = dataPack.dataCore.norm ? pTtlMaxNorm : pTtlMaxErr;
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
