package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IKeyConverter;
import com.soybeany.cache.v2.exception.BdCacheException;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.model.*;

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
        CacheEntity<Data> cacheEntity = CacheEntity.fromDataPack(dataPack, onGetCurTimestamp(), onGetTtl(param, dataPack.dataCore));
        CacheEntity<Data> newCacheEntity = onSaveCacheEntity(param, getStorageKey(param), cacheEntity);
        return onRewriteCacheData(cacheEntity, newCacheEntity, dataPack);
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

}
