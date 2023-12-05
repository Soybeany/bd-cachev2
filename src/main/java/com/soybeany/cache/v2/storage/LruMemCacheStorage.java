package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.exception.BdCacheException;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.model.CacheEntity;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataCore;

import java.lang.ref.WeakReference;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

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
        Optional<CacheEntity<Data>> entityOpt = mapStorage.onLoad(key);
        if (!entityOpt.isPresent()) {
            throw new NoCacheException();
        }
        return entityOpt.get();
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

    public static class Builder<Param, Data> extends StdStorageBuilder<Param, Data> {
        /**
         * 设置用于存放数据的队列容量
         */
        protected int capacity = 100;

        public Builder<Param, Data> capacity(int capacity) {
            this.capacity = capacity;
            return this;
        }

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

        Optional<CacheEntity<Data>> onLoad(String key);

        void onSave(String key, CacheEntity<Data> entity);

        default <T> Optional<T> load(LruMap<String, WeakReference<T>> lruMap, String key) {
            WeakReference<T> reference = lruMap.get(key);
            if (null != reference) {
                T data;
                // 找到具体的数据，返回
                if (null != (data = reference.get())) {
                    return Optional.of(data);
                }
                // 只剩空壳，移除
                lruMap.remove(key);
            }
            // 没有找到key，返回空
            return Optional.empty();
        }

        default <T> void save(LruMap<String, WeakReference<T>> lruMap, String key, T value) {
            lruMap.put(key, new WeakReference<>(value));
        }
    }

    private static class SimpleImpl<Data> implements MapStorage<Data> {

        private final LruMap<String, WeakReference<CacheEntity<Data>>> lruMap;

        public SimpleImpl(LruMap<String, WeakReference<CacheEntity<Data>>> lruMap) {
            this.lruMap = lruMap;
        }

        @Override
        public Map<String, ?> getMap() {
            return lruMap;
        }

        @Override
        public Optional<CacheEntity<Data>> onLoad(String key) {
            return load(lruMap, key);
        }

        @Override
        public void onSave(String key, CacheEntity<Data> entity) {
            save(lruMap, key, entity);
        }
    }

    private static class StringImpl<Data> implements MapStorage<Data> {

        private final LruMap<String, WeakReference<Holder>> lruMap;
        private final Type type;

        public StringImpl(LruMap<String, WeakReference<Holder>> lruMap, Type type) {
            this.lruMap = lruMap;
            this.type = type;
        }

        @Override
        public Map<String, ?> getMap() {
            return lruMap;
        }

        @Override
        public Optional<CacheEntity<Data>> onLoad(String key) {
            Optional<Holder> holderOpt = load(lruMap, key);
            if (!holderOpt.isPresent()) {
                return Optional.empty();
            }
            Holder holder = holderOpt.get();
            DataCore<Data> core;
            try {
                core = DataCore.fromJson(holder.DataCoreJson, type);
            } catch (ClassNotFoundException e) {
                throw new BdCacheException("反序列化数据对象异常:" + e.getMessage());
            }
            return Optional.of(new CacheEntity<>(core, holder.pExpireAt));
        }

        @Override
        public void onSave(String key, CacheEntity<Data> entity) {
            save(lruMap, key, new Holder(DataCore.toJson(entity.dataCore), entity.pExpireAt));
        }
    }

    private static class Holder {
        final String DataCoreJson;
        final long pExpireAt;

        public Holder(String dataCoreJson, long pExpireAt) {
            DataCoreJson = dataCoreJson;
            this.pExpireAt = pExpireAt;
        }
    }

}
