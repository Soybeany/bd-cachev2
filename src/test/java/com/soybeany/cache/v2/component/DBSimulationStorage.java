package com.soybeany.cache.v2.component;

import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.model.CacheEntity;
import com.soybeany.cache.v2.model.DataParam;
import com.soybeany.cache.v2.storage.StdStorage;

import java.util.HashMap;
import java.util.Map;

/**
 * <br>Created by Soybeany on 2020/10/16.
 */
public class DBSimulationStorage<Param, Data> extends StdStorage<Param, Data> {
    private final Map<String, Holder> map = new HashMap<>();

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
    protected CacheEntity<Data> onLoadCacheEntity(DataParam<Param> param, String storageKey) throws NoCacheException {
        if (!map.containsKey(storageKey)) {
            throw new NoCacheException();
        }
        return map.get(storageKey).entity;
    }

    @Override
    protected CacheEntity<Data> onSaveCacheEntity(DataParam<Param> param, String storageKey, CacheEntity<Data> entity) {
        map.put(storageKey, new Holder(param.value, entity));
        return entity;
    }

    @Override
    protected void onRemoveCacheEntity(DataParam<Param> param, String storageKey) {
        map.remove(storageKey);
    }

    @Override
    protected long onGetCurTimestamp() {
        return System.currentTimeMillis();
    }

    @Override
    public void onClearCache() {
        map.clear();
    }

    @Override
    public int cachedDataCount() {
        return map.size();
    }

    // ***********************内部类****************************

    private class Holder {
        Param param;
        CacheEntity<Data> entity;

        public Holder(Param param, CacheEntity<Data> entity) {
            this.param = param;
            this.entity = entity;
        }
    }
}
