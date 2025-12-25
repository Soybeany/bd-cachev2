package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.frame.ILockSupport;
import com.soybeany.cache.v2.exception.BdCacheException;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.model.CacheEntity;
import com.soybeany.cache.v2.model.DataCore;
import com.soybeany.cache.v2.model.DataParam;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

/**
 * @author Soybeany
 * @date 2022/2/9
 */
public class LruMemCacheStorage<Param, Data> extends StdStorage<Param, Data> implements ILockSupport<Lock, Object> {
    private static final String DESC = "LRU";

    private final ILockSupport<Lock, Object> locker;
    private final MapStorage<Data> mapStorage;
    private final Type deppCopyType;

    public LruMemCacheStorage(long pTtl, long pTtlErr, int capacity) {
        this(pTtl, pTtlErr, capacity, null);
    }

    public LruMemCacheStorage(long pTtl, long pTtlErr, int capacity, Type deppCopyType) {
        this(pTtl, pTtlErr, capacity, deppCopyType, new ReentrantLockSupport(DESC));
    }

    public LruMemCacheStorage(long pTtl, long pTtlErr, int capacity, Type deppCopyType, ILockSupport<Lock, Object> locker) {
        this(pTtl, pTtlErr, deppCopyType, locker, new SimpleImpl<>(new LruMap<>(capacity)));
    }

    private LruMemCacheStorage(long pTtl, long pTtlErr, Type deppCopyType, ILockSupport<Lock, Object> locker, MapStorage<Data> storage) {
        super(pTtl, pTtlErr);
        this.locker = locker;
        this.deppCopyType = deppCopyType;
        mapStorage = storage;
    }

    @Override
    public String desc() {
        return DESC;
    }

    @Override
    public synchronized void onInvalidAllCache() {
        Set<String> keys = new HashSet<>(mapStorage.getMap().keySet());
        keys.forEach(key -> mapStorage.onLoad(key).ifPresent(entity -> mapStorage.onSave(key, new CacheEntity<>(entity.dataCore, 0))));
    }

    @Override
    public synchronized void onClearCache() {
        mapStorage.getMap().clear();
    }

    @Override
    public int cachedDataCount() {
        return mapStorage.getMap().size();
    }

    @Override
    protected synchronized CacheEntity<Data> onLoadCacheEntity(DataParam<Param> param, String storageKey) throws NoCacheException {
        Optional<CacheEntity<Data>> entityOpt = mapStorage.onLoad(storageKey);
        if (!entityOpt.isPresent()) {
            throw new NoCacheException();
        }
        CacheEntity<Data> result = entityOpt.get();
        if (null != deppCopyType) {
            String coreJson = DataCore.toJson(result.dataCore);
            try {
                result = new CacheEntity<>(DataCore.fromJson(coreJson, deppCopyType), result.pExpireAt);
            } catch (Exception e) {
                throw new BdCacheException("LoadCache异常:" + e.getMessage());
            }
        }
        return result;
    }

    @Override
    protected synchronized CacheEntity<Data> onSaveCacheEntity(DataParam<Param> param, String storageKey, CacheEntity<Data> entity) {
        mapStorage.onSave(storageKey, entity);
        return entity;
    }

    @Override
    protected void onRemoveCacheEntity(DataParam<Param> param, String storageKey) {
        mapStorage.getMap().remove(storageKey);
    }

    @Override
    protected long onGetCurTimestamp() {
        return System.currentTimeMillis();
    }

    @Override
    public long getNextCheckStamp(DataParam<Param> param) {
        return mapStorage.onLoad(getStorageKey(param))
                .map(entity -> entity.pNextCheckAt)
                .orElse(0L);
    }

    @Override
    public void setNextCheckStamp(DataParam<Param> param, long stamp) {
        mapStorage.onLoad(getStorageKey(param))
                .ifPresent(entity -> entity.pNextCheckAt = stamp);
    }

    @Override
    public Lock onTryLock(String key) {
        return locker.onTryLock(key);
    }

    @Override
    public void onUnlock(Lock lock) {
        locker.onUnlock(lock);
    }

    @Override
    public Object onTryLockAll() {
        return locker.onTryLockAll();
    }

    @Override
    public void onUnlockAll(Object lock) {
        locker.onUnlockAll(lock);
    }

    // ***********************内部类****************************

    public static class Builder<Param, Data> extends StdStorageBuilder<Param, Data> {
        /**
         * 设置用于存放数据的队列容量
         */
        protected int capacity = 100;
        protected boolean weakRef;
        protected Function<String, Long> lockWaitTimeSingleSupplier;
        protected Long lockWaitTimeAll;
        protected Type deppCopyType;

        public Builder<Param, Data> capacity(int capacity) {
            this.capacity = capacity;
            return this;
        }

        public Builder<Param, Data> weakRef(boolean flag) {
            weakRef = flag;
            return this;
        }

        @SuppressWarnings("unused")
        public Builder<Param, Data> lockWaitTimeSingle(Function<String, Long> supplier) {
            lockWaitTimeSingleSupplier = supplier;
            return this;
        }

        @SuppressWarnings("unused")
        public Builder<Param, Data> lockWaitTimeAll(long millis) {
            lockWaitTimeAll = millis;
            return this;
        }

        @SuppressWarnings("unused")
        public Builder<Param, Data> deepCopy(Type type) {
            this.deppCopyType = type;
            return this;
        }

        @Override
        protected ICacheStorage<Param, Data> onBuild() {
            RefImpl<Data> storage = weakRef ? new RefImpl<>(capacity, WeakReference::new) : new RefImpl<>(capacity, SoftReference::new);
            return new LruMemCacheStorage<>(pTtl, pTtlErr, deppCopyType, new ReentrantLockSupport(DESC, lockWaitTimeSingleSupplier, lockWaitTimeAll), storage);
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
