package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IKeyConverter;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.model.CacheEntity;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.model.DataParam;

/**
 * @author Soybeany
 * @date 2022/2/9
 */
public abstract class StdStorage<Param, Data> implements ICacheStorage<Param, Data> {

    private final IKeyConverter<String> storageKeyConverter = onSetupStorageKeyConverter();
    protected final long pTtl;
    protected final long pTtlErr;

    protected DataContext<Param> context;
    private boolean enableRenewExpiredCache;

    public StdStorage(long pTtl, long pTtlErr) {
        this.pTtl = pTtl;
        this.pTtlErr = pTtlErr;
    }

    @Override
    public void onInit(DataContext<Param> context) {
        this.context = context;
    }

    @Override
    public DataPack<Data> onGetCache(DataParam<Param> param) throws NoCacheException {
        String key = getStorageKey(param);
        CacheEntity<Data> cacheEntity = onLoadCacheEntity(param, key);
        long curTimestamp = onGetCurTimestamp();
        // 若缓存中的数据过期，则移除数据后抛出无数据异常
        if (cacheEntity.isExpired(curTimestamp)) {
            if (!enableRenewExpiredCache) {
                onRemoveCacheEntity(param, key);
            }
            throw new NoCacheException();
        }
        // 返回数据
        return CacheEntity.toDataPack(cacheEntity, this, curTimestamp);
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
            // 重新持久化一个使用新过期时间的info
            CacheEntity<Data> newCacheEntity = new CacheEntity<>(cacheEntity.dataCore, curTimestamp + pTtlErr);
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

    private DataPack<Data> simpleCacheData(DataParam<Param> param, String storageKey, DataPack<Data> dataPack) {
        CacheEntity<Data> cacheEntity = CacheEntity.fromDataPack(dataPack, onGetCurTimestamp(), pTtl, pTtlErr);
        CacheEntity<Data> newCacheEntity = onSaveCacheEntity(param, storageKey, cacheEntity);
        return onRewriteCacheData(cacheEntity, newCacheEntity, dataPack);
    }

}
