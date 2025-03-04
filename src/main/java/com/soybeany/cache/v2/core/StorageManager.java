package com.soybeany.cache.v2.core;

import com.soybeany.cache.v2.contract.ICacheChecker;
import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.contract.IDatasource;
import com.soybeany.cache.v2.exception.BdCacheException;
import com.soybeany.cache.v2.exception.CacheWaitException;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.exception.NoDataSourceException;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataCore;
import com.soybeany.cache.v2.model.DataPack;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class StorageManager<Param, Data> {

    /**
     * 默认的锁等待时间
     */
    public static final long LOCK_WAIT_TIME_DEFAULT = 30;

    private final Map<String, Lock> mKeyMap = new WeakHashMap<>();
    private final LinkedList<ICacheStorage<Param, Data>> storages = new LinkedList<>();

    private ICheckHolder<Param, Data> checkerHolder = (context, pack) -> false;
    private long lockWaitTime = LOCK_WAIT_TIME_DEFAULT;
    private boolean enableRenewExpiredCache;

    public static <Param, Data> DataPack<Data> getDataDirectly(Object noDatasourceInvoker, Param param, IDatasource<Param, Data> datasource) {
        // 没有指定数据源
        if (null == datasource) {
            return new DataPack<>(DataCore.fromException(new NoDataSourceException()), noDatasourceInvoker, Long.MAX_VALUE);
        }
        // 正常执行
        try {
            Data data = datasource.onGetData(param);
            return new DataPack<>(DataCore.fromData(data), datasource, datasource.onSetupExpiry(param, data));
        } catch (RuntimeException e) {
            return new DataPack<>(DataCore.fromException(e), datasource, datasource.onSetupExpiry(param, e));
        }
    }

    public void addStorage(ICacheStorage<Param, Data> storage) {
        storages.add(storage);
    }

    public void setDataChecker(long minInterval, ICacheChecker<Param, Data> checker) {
        checkerHolder = new ICheckHolder<Param, Data>() {
            @Override
            public boolean needUpdate(DataContext<Param> context, DataPack<Data> dataPack) {
                if (storages.isEmpty()) {
                    return false;
                }
                if (isFromDatasource(dataPack)) {
                    onUpdated(context);
                    return false;
                }
                ICacheStorage<Param, Data> firstStorage = storages.get(0);
                long curTimestamp = System.currentTimeMillis();
                if (curTimestamp < firstStorage.getNextCheckStamp(context)) {
                    return false;
                }
                return StorageManager.this.needUpdate(checker, context, dataPack);
            }

            @Override
            public void onUpdated(DataContext<Param> context) {
                ICacheStorage<Param, Data> firstStorage = storages.get(0);
                long curTimestamp = System.currentTimeMillis();
                firstStorage.setNextCheckStamp(context, curTimestamp + minInterval);
            }
        };
    }

    public List<ICacheStorage<Param, Data>> storages() {
        return storages;
    }

    public boolean enableRenewExpiredCache() {
        return enableRenewExpiredCache;
    }

    public void enableRenewExpiredCache(boolean enableRenewExpiredCache) {
        this.enableRenewExpiredCache = enableRenewExpiredCache;
    }

    public void lockWaitTime(long lockWaitTime) {
        this.lockWaitTime = lockWaitTime;
    }

    public void init(DataContext.Core<Param, Data> core) {
        if (storages.isEmpty()) {
            return;
        }
        storages.getLast().enableRenewExpiredCache(enableRenewExpiredCache);
        storages.forEach(storage -> storage.onInit(core));
    }

    /**
     * 获取数据并自动缓存
     */
    public DataPack<Data> getDataPack(DataContext<Param> context, IDatasource<Param, Data> datasource, boolean needStore) {
        return exeWithLock(context, () -> {
                    DataPack<Data> dataPack = onGetDataPack(context, datasource, needStore);
                    boolean needUpdate = checkerHolder.needUpdate(context, dataPack);
                    if (needUpdate) {
                        dataPack = onGetDataPack(context, datasource, needStore);
                        checkerHolder.onUpdated(context);
                    }
                    return dataPack;
                },
                e -> new DataPack<>(DataCore.fromException(e), this, 0)
        );
    }

    public void cacheData(DataContext<Param> context, DataPack<Data> dataPack) {
        exeWithLock(context, () -> {
            traverseR((storage, prePack) -> storage.onCacheData(context, prePack), dataPack);
            checkerHolder.onUpdated(context);
        });
    }

    public void batchCacheData(DataContext.Core<Param, Data> contextCore, Map<DataContext.Param<Param>, DataPack<Data>> dataPacks) {
        Set<DataContext.Param<Param>> params = dataPacks.keySet();
        List<String> keys = params.stream().map(param -> param.paramKey).collect(Collectors.toList());
        exeWithLocks(keys, () -> {
            traverseR((storage, prePacks) -> storage.onBatchCacheData(contextCore, prePacks), dataPacks);
            params.forEach(param -> checkerHolder.onUpdated(new DataContext<>(contextCore, param)));
        });
    }

    public void invalidCache(DataContext<Param> context, int... storageIndexes) {
        exeWithLock(context, () -> onInvalidCache(context, storageIndexes));
    }

    public void invalidAllCache(DataContext.Core<Param, Data> contextCore, int... storageIndexes) {
        exeWithLocks(mKeyMap.keySet(), () -> traverse(storage -> storage.onInvalidAllCache(contextCore), storageIndexes));
    }

    public void removeCache(DataContext<Param> context, int... storageIndexes) {
        exeWithLock(context, () -> traverse(storage -> storage.onRemoveCache(context), storageIndexes));
    }

    public void clearCache(DataContext.Core<Param, Data> contextCore, int... storageIndexes) {
        exeWithLocks(mKeyMap.keySet(), () -> traverse(storage -> storage.onClearCache(contextCore), storageIndexes));
    }

    public boolean checkCache(DataContext<Param> context, ICacheChecker<Param, Data> checker, IDatasource<Param, Data> datasource) {
        return exeWithLock2(context, () -> needUpdate(checker, context, onGetDataPack(context, datasource, true)));
    }

    // ****************************************内部方法****************************************

    private <T> void traverseR(ICallback1<Param, Data, T> callback, T data) {
        for (int i = storages.size() - 1; i >= 0; i--) {
            data = callback.onInvoke(storages.get(i), data);
        }
    }

    private void traverse(ICallback2<Param, Data> callback, int... storageIndexes) {
        Predicate<Integer> filter = i -> true;
        if (null != storageIndexes && storageIndexes.length > 0) {
            Set<Integer> indexes = Arrays.stream(storageIndexes).boxed().collect(Collectors.toSet());
            filter = indexes::contains;
        }
        for (int i = 0; i < storages.size(); i++) {
            if (filter.test(i)) {
                callback.onInvoke(storages.get(i));
            }
        }
    }

    private boolean needUpdate(ICacheChecker<Param, Data> checker, DataContext<Param> context, DataPack<Data> dataPack) {
        boolean needUpdate = checker.needUpdate(context.param.param, dataPack);
        context.core.logger.onCheckCache(context, needUpdate);
        if (needUpdate) {
            onInvalidCache(context);
        }
        return needUpdate;
    }

    private DataPack<Data> onGetDataPack(DataContext<Param> context, IDatasource<Param, Data> datasource, boolean needStore) {
        List<ICacheStorage<Param, Data>> storeList = new ArrayList<>();
        DataPack<Data> dataPack = null;
        for (ICacheStorage<Param, Data> storage : storages) {
            try {
                dataPack = storage.onGetCache(context);
                break;
            }
            // 若当前节点没有缓存，则将当前节点纳入到待存储列表
            catch (NoCacheException e) {
                if (needStore) {
                    storeList.add(storage);
                }
            }
        }
        // 若全部节点均没有缓存，则直接访问数据源
        if (null == dataPack) {
            dataPack = getDataDirectly(this, context.param.param, datasource);
        }
        // 回写缓存
        for (int i = storeList.size() - 1; i >= 0; i--) {
            dataPack = storeList.get(i).onCacheData(context, dataPack);
        }
        return dataPack;
    }

    private void onInvalidCache(DataContext<Param> context, int... storageIndexes) {
        traverse(storage -> storage.onInvalidCache(context), storageIndexes);
    }

    private void exeWithLock(DataContext<Param> context, Runnable callback) {
        exeWithLock(context, toFunc(callback), getOnException());
    }

    private <T> T exeWithLock2(DataContext<Param> context, Supplier<T> callback) {
        return exeWithLock(context, callback, getOnException());
    }

    private <T> T exeWithLock(DataContext<Param> context, Supplier<T> callback, Function<RuntimeException, T> onException) {
        return exeWithLock(() -> tryLock(getLock(context.param.paramKey)), Lock::unlock, callback, onException);
    }

    private void exeWithLocks(Collection<String> keys, Runnable callback) {
        exeWithLock(() -> {
            LocksHolder holder = new LocksHolder(keys.stream().map(this::getLock).collect(Collectors.toList()));
            holder.locks.stream().map(this::tryLock).forEach(holder.locking::add);
            return holder;
        }, holder -> holder.locking.forEach(Lock::unlock), toFunc(callback), getOnException());
    }

    private boolean isFromDatasource(DataPack<Data> dataPack) {
        return dataPack.provider instanceof IDatasource;
    }

    private <L, T> T exeWithLock(Supplier<L> onLock, Consumer<L> onUnlock, Supplier<T> callback, Function<RuntimeException, T> onException) {
        // 加锁，避免并发时数据重复获取
        L lock;
        try {
            lock = onLock.get();
        } catch (RuntimeException e) {
            return onException.apply(e);
        }
        // 执行数据获取逻辑
        try {
            return callback.get();
        } catch (RuntimeException e) {
            return onException.apply(e);
        } finally {
            onUnlock.accept(lock);
        }
    }

    private Lock tryLock(Lock lock) {
        try {
            if (!lock.tryLock(lockWaitTime, TimeUnit.SECONDS)) {
                throw new CacheWaitException("超时");
            }
        } catch (InterruptedException e) {
            throw new CacheWaitException("中断");
        }
        return lock;
    }

    private <T> Supplier<T> toFunc(Runnable callback) {
        return () -> {
            callback.run();
            return null;
        };
    }

    private <T> Function<RuntimeException, T> getOnException() {
        return e -> {
            throw new BdCacheException(e.getMessage());
        };
    }

    private Lock getLock(String key) {
        synchronized (mKeyMap) {
            return mKeyMap.computeIfAbsent(key, k -> new ReentrantLock());
        }
    }

    // ****************************************内部类****************************************

    private interface ICallback1<Param, Data, T> {
        T onInvoke(ICacheStorage<Param, Data> storage, T data);
    }

    private interface ICallback2<Param, Data> {
        void onInvoke(ICacheStorage<Param, Data> storage);
    }

    private interface ICheckHolder<Param, Data> {
        boolean needUpdate(DataContext<Param> context, DataPack<Data> dataPack);

        default void onUpdated(DataContext<Param> context) {
        }
    }

    private static class LocksHolder {
        final Collection<Lock> locks;
        public final List<Lock> locking = new ArrayList<>();

        LocksHolder(List<Lock> locks) {
            this.locks = locks;
        }
    }
}
