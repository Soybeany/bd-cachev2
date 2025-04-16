package com.soybeany.cache.v2.core;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.ICacheChecker;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.contract.user.IOnInvalidListener;
import com.soybeany.cache.v2.exception.BdCacheException;
import com.soybeany.cache.v2.exception.CacheWaitException;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.exception.NoDataSourceException;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataCore;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.model.DataParam;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class StorageManager<Param, Data> {

    /**
     * 默认的锁等待时间
     */
    public static final long LOCK_WAIT_TIME_DEFAULT = 30;

    private final Map<String, Lock> mKeyMap = new WeakHashMap<>();
    private final LinkedList<ICacheStorage<Param, Data>> storages = new LinkedList<>();
    private final Set<IOnInvalidListener<Param>> onInvalidListeners = new HashSet<>();

    private DataContext context;
    private ICheckHolder<Param, Data> checkerHolder = (param, supplier) -> supplier.get();
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

    public void setDataChecker(Function<Param, Long> intervalSupplier, ICacheChecker<Param, Data> checker) {
        checkerHolder = new ICheckHolder<Param, Data>() {
            @Override
            public DataPack<Data> getCheckedDataPack(DataParam<Param> param, Supplier<DataPack<Data>> supplier) {
                DataPack<Data> dataPack = supplier.get();
                // 没有缓存策略，直接返回
                if (storages.isEmpty()) {
                    return dataPack;
                }
                // 来源于数据源，只更新下次检测时间，直接返回
                if (isFromDatasource(dataPack)) {
                    updateNextCheckTime(param);
                    return dataPack;
                }

                ICacheStorage<Param, Data> firstStorage = storages.get(0);
                long curTimestamp = System.currentTimeMillis();
                // 若没到检测时间，则不作处理
                if (curTimestamp < firstStorage.getNextCheckStamp(param)) {
                    return dataPack;
                }
                // 已达检测时间，执行检测
                boolean needUpdate = StorageManager.this.needUpdate(checker, param, dataPack);
                // 若需要更新，则重新获取一次数据
                if (needUpdate) {
                    dataPack = supplier.get();
                }
                // 由于已执行检测，重新设置下次检测的时间
                updateNextCheckTime(param);
                return dataPack;
            }

            @Override
            public void updateNextCheckTime(DataParam<Param> param) {
                ICacheStorage<Param, Data> firstStorage = storages.get(0);
                long curTimestamp = System.currentTimeMillis();
                firstStorage.setNextCheckStamp(param, curTimestamp + intervalSupplier.apply(param.value));
            }
        };
    }

    public void addOnInvalidListener(IOnInvalidListener<Param> listener) {
        onInvalidListeners.add(listener);
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

    public void init(DataContext context) {
        this.context = context;
        if (storages.isEmpty()) {
            return;
        }
        storages.getLast().enableRenewExpiredCache(enableRenewExpiredCache);
        storages.forEach(storage -> storage.onInit(context));
    }

    /**
     * 获取数据并自动缓存
     */
    public DataPack<Data> getDataPack(DataParam<Param> param, IDatasource<Param, Data> datasource, boolean needStore) {
        return exeWithLock(param, () -> checkerHolder.getCheckedDataPack(param, () -> onGetDataPack(param, datasource, needStore)),
                e -> new DataPack<>(DataCore.fromException(e), this, 0)
        );
    }

    public void cacheData(DataParam<Param> param, DataPack<Data> dataPack) {
        exeWithLock(param, () -> {
            traverseR((storage, prePack) -> storage.onCacheData(param, prePack), dataPack);
            checkerHolder.updateNextCheckTime(param);
        });
    }

    public void batchCacheData(Map<DataParam<Param>, DataPack<Data>> dataPacks) {
        Set<DataParam<Param>> params = dataPacks.keySet();
        List<String> keys = params.stream().map(param -> param.paramKey).collect(Collectors.toList());
        exeWithLocks(keys, () -> {
            traverseR(ICacheStorage::onBatchCacheData, dataPacks);
            params.forEach(param -> checkerHolder.updateNextCheckTime(param));
        });
    }

    public void invalidCache(DataParam<Param> param, int... storageIndexes) {
        exeWithLock(param, () -> onInvalidCache(param, storageIndexes));
    }

    public void invalidAllCache(int... storageIndexes) {
        exeWithLocks(mKeyMap.keySet(), () -> traverse(ICacheStorage::onInvalidAllCache, storageIndexes));
    }

    public void removeCache(DataParam<Param> param, int... storageIndexes) {
        exeWithLock(param, () -> traverse(storage -> storage.onRemoveCache(param), storageIndexes));
    }

    public void clearCache(int... storageIndexes) {
        exeWithLocks(mKeyMap.keySet(), () -> traverse(ICacheStorage::onClearCache, storageIndexes));
    }

    public boolean checkCache(DataParam<Param> param, ICacheChecker<Param, Data> checker) {
        return exeWithLock2(param, () -> {
            DataPack<Data> dataPack = onGetDataPack(param, null, false);
            if (dataPack.dataCore.exception instanceof NoDataSourceException) {
                return false;
            }
            boolean needUpdate = needUpdate(checker, param, dataPack);
            checkerHolder.updateNextCheckTime(param);
            return needUpdate;
        });
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

    private boolean needUpdate(ICacheChecker<Param, Data> checker, DataParam<Param> param, DataPack<Data> dataPack) {
        boolean needUpdate = checker.needUpdate(param.value, dataPack);
        context.logger.onCheckCache(param, needUpdate);
        if (needUpdate) {
            onInvalidCache(param);
        }
        return needUpdate;
    }

    private DataPack<Data> onGetDataPack(DataParam<Param> param, IDatasource<Param, Data> datasource, boolean needStore) {
        List<ICacheStorage<Param, Data>> storeList = new ArrayList<>();
        DataPack<Data> dataPack = null;
        // todo 每个Storage，均支持其提供锁（不提供则表示不需要锁），最后统一反向解锁
        for (ICacheStorage<Param, Data> storage : storages) {
            try {
                dataPack = storage.onGetCache(param);
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
            dataPack = getDataDirectly(this, param.value, datasource);
        }
        // 回写缓存
        for (int i = storeList.size() - 1; i >= 0; i--) {
            dataPack = storeList.get(i).onCacheData(param, dataPack);
        }
        return dataPack;
    }

    private void onInvalidCache(DataParam<Param> param, int... storageIndexes) {
        traverse(storage -> storage.onInvalidCache(param), storageIndexes);
        onInvalidListeners.forEach(listener -> listener.onInvoke(param.value, storageIndexes));
    }

    private void exeWithLock(DataParam<Param> param, Runnable callback) {
        exeWithLock(param, toFunc(callback), getOnException());
    }

    private <T> T exeWithLock2(DataParam<Param> param, Supplier<T> callback) {
        return exeWithLock(param, callback, getOnException());
    }

    private <T> T exeWithLock(DataParam<Param> param, Supplier<T> callback, Function<RuntimeException, T> onException) {
        return exeWithLock(() -> tryLock(getLock(param.paramKey)), Lock::unlock, callback, onException);
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
        DataPack<Data> getCheckedDataPack(DataParam<Param> param, Supplier<DataPack<Data>> supplier);

        default void updateNextCheckTime(DataParam<Param> param) {
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
