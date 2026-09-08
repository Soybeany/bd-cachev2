package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IKeyConverter;
import com.soybeany.cache.v2.exception.BdCacheException;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.model.CacheEntity;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataCore;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.model.DataParam;

import java.util.Optional;
import java.util.function.BiFunction;

/**
 * @author Soybeany
 * @date 2022/2/9
 */
public abstract class StdStorage<Param, Data> implements ICacheStorage<Param, Data> {

    private final IKeyConverter<String> storageKeyConverter = onSetupStorageKeyConverter();

    /**
     * 有效期函数，入参为缓存的数据核心，返回该数据/异常在该级缓存中的有效期(单位：毫秒)
     */
    private final BiFunction<Param, DataCore<Data>, Long> ttlFunction;

    protected DataContext context;
    private boolean enableRenewExpiredCache;

    public StdStorage(long pTtl, long pTtlErr) {
        this((param, dataCore) -> dataCore.norm ? pTtl : pTtlErr);
    }

    public StdStorage(BiFunction<Param, DataCore<Data>, Long> ttlFunction) {
        this.ttlFunction = Optional.ofNullable(ttlFunction)
                .orElseThrow(() -> new BdCacheException("ttlFunction不能为null"));
    }

    @Override
    public void onInit(DataContext context) {
        this.context = context;
    }

    @Override
    public DataPack<Data> onGetCache(DataParam<Param> param) throws NoCacheException {
        String key = getStorageKey(param);
        CacheEntity<Data> cacheEntity = onLoadCacheEntity(param, key);
        long curTimestamp = onGetCurTimestamp();
        // 若缓存中的数据过期，则抛出无数据异常（不删除，给 fallback 等场景保留回退的可能）
        if (cacheEntity.isExpired(curTimestamp)) {
            throw new NoCacheException();
        }
        // 返回数据
        return CacheEntity.toDataPack(cacheEntity, this, curTimestamp);
    }

    @Override
    public DataPack<Data> onGetCacheIgnoreExpiry(DataParam<Param> param) throws NoCacheException {
        String key = getStorageKey(param);
        CacheEntity<Data> cacheEntity = onLoadCacheEntity(param, key);
        return CacheEntity.toDataPack(cacheEntity, this, onGetCurTimestamp());
    }

    @Override
    public DataPack<Data> onCacheData(DataParam<Param> param, DataPack<Data> dataPack) {
        String key = getStorageKey(param);
        // 若不支持缓存刷新，则不作额外处理
        if (dataPack.norm() || !enableRenewExpiredCache) {
            return simpleCacheData(param, key, dataPack);
        }
        try {
            CacheEntity<Data> cacheEntity = onLoadCacheEntity(param, key);
            // 若缓存依旧可用，则直接使用
            long curTimestamp = onGetCurTimestamp();
            if (!cacheEntity.isExpired(curTimestamp)) {
                return CacheEntity.toDataPack(cacheEntity, this, curTimestamp);
            }
            // 不是正常数据，则当缓存不存在处理
            if (!cacheEntity.dataCore.norm) {
                throw new NoCacheException();
            }
            // 重新持久化一个使用新过期时间的info（续期使用异常的有效期，与原pTtlErr语义一致）
            CacheEntity<Data> newCacheEntity = new CacheEntity<>(cacheEntity.dataCore, curTimestamp + onGetTtl(param, dataPack.dataCore));
            onSaveCacheEntity(param, key, newCacheEntity);
            if (null != context.logger) {
                context.logger.onRenewExpiredCache(param, this);
            }
            return CacheEntity.toDataPack(newCacheEntity, this, curTimestamp);
        } catch (NoCacheException e) {
            // 没有本地缓存，按常规处理
            return simpleCacheData(param, key, dataPack);
        }
    }

    @Override
    public void onInvalidCache(DataParam<Param> param) {
        String key = getStorageKey(param);
        try {
            CacheEntity<Data> cacheEntity = onLoadCacheEntity(param, key);
            onSaveCacheEntity(param, key, new CacheEntity<>(cacheEntity.dataCore, 0));
        } catch (NoCacheException ignore) {
        }
    }

    @Override
    public void onRemoveCache(DataParam<Param> param) {
        onRemoveCacheEntity(param, getStorageKey(param));
    }

    @Override
    public void enableRenewExpiredCache(boolean enable) {
        enableRenewExpiredCache = enable;
    }


    // ***********************子类重写****************************

    /**
     * 允许子类重新定义读取/存储时的key
     */
    protected IKeyConverter<String> onSetupStorageKeyConverter() {
        return key -> key;
    }

    protected String getStorageKey(DataParam<Param> param) {
        return storageKeyConverter.getKey(param.paramKey);
    }

    protected DataPack<Data> onRewriteCacheData(CacheEntity<Data> cacheEntity, CacheEntity<Data> newCacheEntity, DataPack<Data> dataPack) {
        if (newCacheEntity == cacheEntity) {
            return CacheEntity.toDataPack(cacheEntity, dataPack.provider, onGetCurTimestamp());
        }
        return CacheEntity.toDataPack(newCacheEntity, this, onGetCurTimestamp());
    }

    protected abstract CacheEntity<Data> onLoadCacheEntity(DataParam<Param> param, String storageKey) throws NoCacheException;

    protected abstract CacheEntity<Data> onSaveCacheEntity(DataParam<Param> param, String storageKey, CacheEntity<Data> entity);

    protected abstract void onRemoveCacheEntity(DataParam<Param> param, String storageKey);

    protected abstract long onGetCurTimestamp();

    // ***********************内部方法****************************

    /**
     * 获得指定数据/异常在该级缓存中的有效期(单位：毫秒)
     */
    protected long onGetTtl(DataParam<Param> param, DataCore<Data> dataCore) {
        Long ttl = ttlFunction.apply(param.value, dataCore);
        if (null == ttl || ttl < 0) {
            throw new BdCacheException("ttlFunction返回了无效的值:" + ttl);
        }
        return ttl;
    }

    private DataPack<Data> simpleCacheData(DataParam<Param> param, String storageKey, DataPack<Data> dataPack) {
        CacheEntity<Data> cacheEntity = CacheEntity.fromDataPack(dataPack, onGetCurTimestamp(), onGetTtl(param, dataPack.dataCore));
        CacheEntity<Data> newCacheEntity = onSaveCacheEntity(param, storageKey, cacheEntity);
        return onRewriteCacheData(cacheEntity, newCacheEntity, dataPack);
    }

}
