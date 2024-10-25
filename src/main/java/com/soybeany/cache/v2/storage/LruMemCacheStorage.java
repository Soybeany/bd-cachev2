package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.model.CacheEntity;
import com.soybeany.cache.v2.model.DataContext;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * @author Soybeany
 * @date 2022/2/9
 */
public class LruMemCacheStorage<Param, Data> extends StdStorage<Param, Data> {

    private final MapStorage<Data> mapStorage;

    public LruMemCacheStorage(long pTtl, long pTtlErr, int capacity) {
        this(pTtl, pTtlErr, new SimpleImpl<>(new LruMap<>(capacity)));
    }

    private LruMemCacheStorage(long pTtl, long pTtlErr, MapStorage<Data> storage) {
        super(pTtl, pTtlErr);
        mapStorage = storage;
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

    // ***********************内部类****************************

    public static class Builder<Param, Data> extends StdStorageBuilder<Param, Data> {
        /**
         * 设置用于存放数据的队列容量
         */
        protected int capacity = 100;
        protected boolean weakRef;

        public Builder<Param, Data> capacity(int capacity) {
            this.capacity = capacity;
            return this;
        }

        public Builder<Param, Data> weakRef(boolean flag) {
            weakRef = flag;
            return this;
        }

        @Override
        protected ICacheStorage<Param, Data> onBuild() {
            return weakRef ?
                    new LruMemCacheStorage<>(pTtl, pTtlErr, new RefImpl<>(capacity, WeakReference::new)) :
                    new LruMemCacheStorage<>(pTtl, pTtlErr, new RefImpl<>(capacity, SoftReference::new));
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

    }

    private static class SimpleImpl<Data> implements MapStorage<Data> {

        private final LruMap<String, CacheEntity<Data>> lruMap;

        public SimpleImpl(LruMap<String, CacheEntity<Data>> lruMap) {
            this.lruMap = lruMap;
        }

        @Override
        public Map<String, ?> getMap() {
            return lruMap;
        }

        @Override
        public Optional<CacheEntity<Data>> onLoad(String key) {
            return Optional.ofNullable(lruMap.get(key));
        }

        @Override
        public void onSave(String key, CacheEntity<Data> entity) {
            lruMap.put(key, entity);
        }
    }

    private static class RefImpl<Data> implements MapStorage<Data> {

        private final LruMap<String, Reference<CacheEntity<Data>>> lruMap;
        private final Function<CacheEntity<Data>, Reference<CacheEntity<Data>>> parser;

        public RefImpl(int capacity, Function<CacheEntity<Data>, Reference<CacheEntity<Data>>> parser) {
            this.lruMap = new LruMap<>(capacity);
            this.parser = parser;
        }

        @Override
        public Map<String, ?> getMap() {
            return lruMap;
        }

        @Override
        public Optional<CacheEntity<Data>> onLoad(String key) {
            Reference<CacheEntity<Data>> reference = lruMap.get(key);
            if (null != reference) {
                CacheEntity<Data> data;
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

        @Override
        public void onSave(String key, CacheEntity<Data> entity) {
            lruMap.put(key, parser.apply(entity));
        }
    }

}
