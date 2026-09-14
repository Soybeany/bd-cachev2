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
     * 数据的创建时间戳(时间点)，即数据首次写入任一缓存层的时间
     * <br>0表示未知(如旧版本写入的缓存数据)
     */
    public final long pCreateAt;

    /**
     * @param pTtlMax 该级缓存允许的最大生存时间(时间段)
     */
    public static <Data> CacheEntity<Data> fromDataPack(DataPack<Data> dataPack, long curTimestamp, long pTtlMax) {
        // 实际使用min(缓存配置值，数据/异常剩余有效期)
        long pTtl = Math.min(dataPack.pTtl, pTtlMax);
        // 创建时间未知时(如旧调用方构造的包)，以首次写入缓存的时间补齐
        long pCreateAt = 0 == dataPack.pCreateAt ? curTimestamp : dataPack.pCreateAt;
        return new CacheEntity<>(dataPack.dataCore, curTimestamp + pTtl, pCreateAt);
    }

    public static <Data> DataPack<Data> toDataPack(CacheEntity<Data> entity, Object provider, long curTimestamp) {
        return new DataPack<>(entity.dataCore, provider, entity.pExpireAt - curTimestamp, entity.pCreateAt);
    }

    public CacheEntity(DataCore<Data> dataCore, long pExpireAt) {
        this(dataCore, pExpireAt, 0);
    }

    public CacheEntity(DataCore<Data> dataCore, long pExpireAt, long pCreateAt) {
        this.dataCore = dataCore;
        this.pExpireAt = pExpireAt;
        this.pCreateAt = pCreateAt;
    }

    public boolean isExpired(long curTimestamp) {
        return curTimestamp > pExpireAt;
    }

}
