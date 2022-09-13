package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.exception.BdCacheException;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.model.CacheEntity;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataCore;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Soybeany
 * @date 2022/2/9
 */
public class LruMemCacheStorage<Param, Data> extends StdStorage<Param, Data> {

    private final int capacity;
    private MapStorage<Data> mapStorage;

    public LruMemCacheStorage(int pTtl, int pTtlErr, int capacity) {
        super(pTtl, pTtlErr);
        this.capacity = capacity;
        mapStorage = new SimpleImpl<>(new LruMap<>(capacity));
    }

    @Override
    public String desc() {
        return "LRU";
    }

    @Override
    protected synchronized CacheEntity<Data> onLoadCacheEntity(DataContext<Param> context, String key) throws NoCacheException {
        if (!mapStorage.containsKey(key)) {
            throw new NoCacheException();
        }
        return mapStorage.onLoad(key);
    }

    @Override
    protected synchronized CacheEntity<Data> onSaveCacheEntity(DataContext<Param> context, String key, CacheEntity<Data> entity) {
        mapStorage.onSave(key, entity);
        return entity;
    }

    @Override
    protected void onRemoveCacheEntity(DataContext<Param> context, String key) {
        mapStorage.getMap().remove(key);
    }

    @Override
    protected long onGetCurTimestamp() {
        return System.currentTimeMillis();
    }

    @Override
    public synchronized void onClearCache(String storageId) {
        mapStorage.getMap().clear();
    }

    @Override
    public int cachedDataCount(String storageId) {
        return mapStorage.getMap().size();
    }

    public LruMemCacheStorage<Param, Data> withDataCopy(Type type) {
        mapStorage = new StringImpl<>(new LruMap<>(capacity), type);
        return this;
    }

    // ***********************内部类****************************

    @Accessors(fluent = true, chain = true)
    public static class Builder<Param, Data> extends StdStorageBuilder<Param, Data> {
        /**
         * 设置用于存放数据的队列容量
         */
        @Setter
        protected int capacity = 100;

        @Override
        protected ICacheStorage<Param, Data> onBuild() {
            return new LruMemCacheStorage<>(pTtl, pTtlErr, capacity);
        }

        @Override
        public LruMemCacheStorage<Param, Data> build() {
            return (LruMemCacheStorage<Param, Data>) super.build();
        }
    }

    private static class LruMap<K, V> extends LinkedHashMap<K, V> {
        private final int capacity;

        public LruMap(int capacity) {
            super(0, 0.75f, true);
            this.capacity = capacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > capacity;
        }
    }

    private interface MapStorage<Data> {
        Map<String, ?> getMap();

        CacheEntity<Data> onLoad(String key);

        void onSave(String key, CacheEntity<Data> entity);

        default boolean containsKey(String key) {
            return getMap().containsKey(key);
        }
    }

    @AllArgsConstructor
    private static class SimpleImpl<Data> implements MapStorage<Data> {

        private final LruMap<String, CacheEntity<Data>> lruMap;

        @Override
        public Map<String, ?> getMap() {
            return lruMap;
        }

        @Override
        public CacheEntity<Data> onLoad(String key) {
            return lruMap.get(key);
        }

        @Override
        public void onSave(String key, CacheEntity<Data> entity) {
            lruMap.put(key, entity);
        }
    }

    @AllArgsConstructor
    private static class StringImpl<Data> implements MapStorage<Data> {

        private final LruMap<String, Holder> lruMap;
        private final Type type;

        @Override
        public Map<String, ?> getMap() {
            return lruMap;
        }

        @Override
        public CacheEntity<Data> onLoad(String key) {
            Holder holder = lruMap.get(key);
            DataCore<Data> core;
            try {
                core = DataCore.fromJson(holder.DataCoreJson, type);
            } catch (ClassNotFoundException e) {
                throw new BdCacheException("反序列化数据对象异常:" + e.getMessage());
            }
            return new CacheEntity<>(core, holder.pExpireAt);
        }

        @Override
        public void onSave(String key, CacheEntity<Data> entity) {
            lruMap.put(key, new Holder(DataCore.toJson(entity.dataCore), entity.pExpireAt));
        }
    }

    @RequiredArgsConstructor
    private static class Holder {
        final String DataCoreJson;
        final long pExpireAt;
    }

}
