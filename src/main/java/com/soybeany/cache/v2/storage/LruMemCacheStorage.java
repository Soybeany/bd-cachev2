package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.model.CacheEntity;
import com.soybeany.cache.v2.model.DataContext;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Soybeany
 * @date 2022/2/9
 */
public class LruMemCacheStorage<Param, Data> extends StdStorage<Param, Data> {

    @SuppressWarnings("rawtypes")
    private static final Map<String, LruMap> MAP = new ConcurrentHashMap<>();

    private LruMap<String, CacheEntity<Data>> mLruMap;
    private final int capacity;
    private final boolean enableShareStorage;

    public LruMemCacheStorage(int pTtl, int pTtlErr, int capacity, boolean enableShareStorage) {
        super(pTtl, pTtlErr);
        this.capacity = capacity;
        this.enableShareStorage = enableShareStorage;
    }

    @Override
    public String desc() {
        return "LRU";
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onInit(String storageId) {
        if (enableShareStorage) {
            mLruMap = MAP.computeIfAbsent(storageId, id -> new LruMap<>(capacity));
        } else {
            mLruMap = new LruMap<>(capacity);
        }
    }

    @Override
    protected synchronized CacheEntity<Data> onLoadCacheEntity(DataContext<Param> context, String key) throws NoCacheException {
        if (!mLruMap.containsKey(key)) {
            throw new NoCacheException();
        }
        return mLruMap.get(key);
    }

    @Override
    protected synchronized CacheEntity<Data> onSaveCacheEntity(DataContext<Param> context, String key, CacheEntity<Data> entity) {
        mLruMap.put(key, entity);
        return entity;
    }

    @Override
    protected void onRemoveCacheEntity(DataContext<Param> context, String key) {
        mLruMap.remove(key);
    }

    @Override
    protected long onGetCurTimestamp() {
        return System.currentTimeMillis();
    }

    @Override
    public synchronized void onClearCache(String storageId) {
        mLruMap.clear();
    }

    @Override
    public int cachedDataCount(String storageId) {
        return mLruMap.size();
    }

    // ***********************内部类****************************

    @Accessors(fluent = true, chain = true)
    public static class Builder<Param, Data> extends StdStorageBuilder<Param, Data> {
        /**
         * 设置用于存放数据的队列容量
         */
        @Setter
        protected int capacity = 100;

        /**
         * 设置storageId相同时，是否允许共享数据
         */
        @Setter
        protected boolean enableShareStorage;

        @Override
        protected ICacheStorage<Param, Data> onBuild() {
            return new LruMemCacheStorage<>(pTtl, pTtlErr, capacity, enableShareStorage);
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

}
