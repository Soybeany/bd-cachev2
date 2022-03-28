package com.soybeany.cache.v2.component;

import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.model.CacheEntity;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.storage.StdStorage;

import java.util.HashMap;
import java.util.Map;

/**
 * <br>Created by Soybeany on 2020/10/16.
 */
public class DBSimulationStorage<Param, Data> extends StdStorage<Param, Data> {
    private final Map<String, CacheEntity<Data>> map = new HashMap<>();

    public DBSimulationStorage() {
        this(Integer.MAX_VALUE);
    }

    public DBSimulationStorage(int pTtl) {
        super(pTtl, pTtl);
    }

    @Override
    public String desc() {
        return "仿数据库";
    }

    @Override
    public void onInit(String storageId) {
        // 不作存储共享
    }

    @Override
    protected CacheEntity<Data> onLoadCacheEntity(DataContext<Param> context, String key) throws NoCacheException {
        if (!map.containsKey(key)) {
            throw new NoCacheException();
        }
        return map.get(key);
    }

    @Override
    protected CacheEntity<Data> onSaveCacheEntity(DataContext<Param> context, String key, CacheEntity<Data> entity) {
        map.put(key, entity);
        return entity;
    }

    @Override
    protected void onRemoveCacheEntity(DataContext<Param> context, String key) {
        map.remove(key);
    }

    @Override
    protected long onGetCurTimestamp() {
        return System.currentTimeMillis();
    }

    @Override
    public void onClearCache(String storageId) {
        map.clear();
    }

    @Override
    public int cachedDataCount(String storageId) {
        return map.size();
    }

}
