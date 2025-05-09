package com.soybeany.cache.v2.core;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.frame.ILockSupport;
import com.soybeany.cache.v2.contract.user.ICacheChecker;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.contract.user.IOnInvalidListener;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.exception.NoDataSourceException;
import com.soybeany.cache.v2.model.*;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class StorageManager<Param, Data> {

    private final LinkedList<ICacheStorage<Param, Data>> storages = new LinkedList<>();
    private final Set<IOnInvalidListener<Param>> onInvalidListeners = new HashSet<>();

    private DataContext context;
    private ICheckHolder<Param, Data> checkerHolder = (param, supplier) -> supplier.get();
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
        return checkerHolder.getCheckedDataPack(param, () -> onGetDataPack(0, param, datasource, needStore, e -> new DataPack<>(DataCore.fromException(e), this, 0)));
    }

    public void cacheData(DataParam<Param> param, DataPack<Data> dataPack) {
        traverseR((storage, prePack) -> exeWithLock(storage, param, () -> storage.onCacheData(param, prePack)), dataPack);
        checkerHolder.updateNextCheckTime(param);
    }

    public void batchCacheData(Map<DataParam<Param>, DataPack<Data>> dataPacks) {
        Set<DataParam<Param>> params = dataPacks.keySet();
        traverseR((storage, prePacks) -> exeWithLockBatch(storage, params, () -> storage.onBatchCacheData(prePacks)), dataPacks);
        params.forEach(param -> checkerHolder.updateNextCheckTime(param));
    }

    public void invalidCache(DataParam<Param> param, int... storageIndexes) {
        traverse(storage -> exeWithLock(storage, param, () -> storage.onInvalidCache(param)), storageIndexes);
        onInvalidListeners.forEach(listener -> listener.onInvoke(param.value, storageIndexes));
    }

    public void invalidAllCache(int... storageIndexes) {
        traverse(storage -> exeWithLockAll(storage, storage::onInvalidAllCache), storageIndexes);
    }

    public void removeCache(DataParam<Param> param, int... storageIndexes) {
        traverse(storage -> exeWithLock(storage, param, () -> storage.onRemoveCache(param)), storageIndexes);
    }

    public void clearCache(int... storageIndexes) {
        traverse(storage -> exeWithLockAll(storage, storage::onClearCache), storageIndexes);
    }

    public boolean checkCache(DataParam<Param> param, ICacheChecker<Param, Data> checker) {
        DataPack<Data> dataPack = onGetDataPack(0, param, null, false, getOnException());
        if (dataPack.dataCore.exception instanceof NoDataSourceException) {
            return false;
        }
        boolean needUpdate = needUpdate(checker, param, dataPack);
        checkerHolder.updateNextCheckTime(param);
        return needUpdate;
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
                ICacheStorage<Param, Data> storage = storages.get(i);
                callback.onInvoke(storage);
            }
        }
    }

    private boolean needUpdate(ICacheChecker<Param, Data> checker, DataParam<Param> param, DataPack<Data> dataPack) {
        boolean needUpdate = checker.needUpdate(param.value, dataPack);
        context.logger.onCheckCache(param, needUpdate);
        if (needUpdate) {
            invalidCache(param);
        }
        return needUpdate;
    }

    private DataPack<Data> onGetDataPack(int storageIndex, DataParam<Param> param, IDatasource<Param, Data> datasource, boolean needStore, Function<RuntimeException, DataPack<Data>> onException) {
        // 若超出storages边界，则访问数据源
        if (storageIndex >= storages.size()) {
            return getDataDirectly(this, param.value, datasource);
        }

        ICacheStorage<Param, Data> storage = storages.get(storageIndex);
        return exeWithLock(storage, param, () -> {
            try {
                // 从当前storage获取数据
                return storage.onGetCache(param);
            } catch (NoCacheException e) {
                // 从下一storage获取数据
                DataPack<Data> dataPack = onGetDataPack(storageIndex + 1, param, datasource, needStore, onException);
                // 按需缓存数据
                if (needStore) {
                    dataPack = storage.onCacheData(param, dataPack);
                }
                return dataPack;
            }
        }, onException);
    }

    private void exeWithLock(ICacheStorage<Param, Data> storage, DataParam<Param> param, Runnable callback) {
        exeWithLock(storage, param, toFunc(callback));
    }

    private <T> T exeWithLock(ICacheStorage<Param, Data> storage, DataParam<Param> param, Supplier<T> callback) {
        return exeWithLock(storage, param, callback, getOnException());
    }

    private <T> T exeWithLock(ICacheStorage<Param, Data> storage, DataParam<Param> param, Supplier<T> callback, Function<RuntimeException, T> onException) {
        return onExeWithLockAuto(storage, lockHelper -> lockHelper.tryLock(param), (lockHelper, lock) -> lockHelper.unlock(param, lock), callback, onException);
    }

    private <T> T exeWithLockBatch(ICacheStorage<Param, Data> storage, Collection<DataParam<Param>> params, Supplier<T> callback) {
        return onExeWithLockAuto(storage, lockHelper -> lockHelper.tryLockBatch(params), LockHelper::unlockBatch, callback, getOnException());
    }

    private void exeWithLockAll(ICacheStorage<Param, Data> storage, Runnable callback) {
        onExeWithLockAuto(storage, LockHelper::tryLockAll, LockHelper::unlockAll, toFunc(callback), getOnException());
    }

    @SuppressWarnings("unchecked")
    private <L, AL, T, K> T onExeWithLockAuto(ICacheStorage<Param, Data> storage, ICallback3<Param, L, AL, K> onLock, ICallback4<Param, L, AL, K> onUnlock, Supplier<T> callback, Function<RuntimeException, T> onException) {
        if (storage instanceof ILockSupport) {
            LockHelper<Param, L, AL> lockHelper = new LockHelper<>(context, (ILockSupport<Param, L, AL>) storage);
            return onExeWithLock(() -> onLock.onInvoke(lockHelper), lock -> onUnlock.onInvoke(lockHelper, lock), callback, onException);
        } else {
            return onExe(callback, onException);
        }
    }

    private <L, T> T onExeWithLock(Supplier<L> onLock, Consumer<L> onUnlock, Supplier<T> callback, Function<RuntimeException, T> onException) {
        // 加锁，避免并发时数据重复获取
        L lock;
        try {
            lock = onLock.get();
        } catch (RuntimeException e) {
            return onException.apply(e);
        }
        // 执行数据获取逻辑
        try {
            return onExe(callback, onException);
        } finally {
            onUnlock.accept(lock);
        }
    }

    private <T> T onExe(Supplier<T> callback, Function<RuntimeException, T> onException) {
        // 执行数据获取逻辑
        try {
            return callback.get();
        } catch (RuntimeException e) {
            return onException.apply(e);
        }
    }

    private boolean isFromDatasource(DataPack<Data> dataPack) {
        return dataPack.provider instanceof IDatasource;
    }

    private <T> Supplier<T> toFunc(Runnable callback) {
        return () -> {
            callback.run();
            return null;
        };
    }

    private <T> Function<RuntimeException, T> getOnException() {
        return e -> {
            throw e;
        };
    }

    // ****************************************内部类****************************************

    private interface ICallback1<Param, Data, T> {
        T onInvoke(ICacheStorage<Param, Data> storage, T data);
    }

    private interface ICallback2<Param, Data> {
        void onInvoke(ICacheStorage<Param, Data> storage);
    }

    private interface ICallback3<Param, L, AL, K> {
        K onInvoke(LockHelper<Param, L, AL> locker);
    }

    private interface ICallback4<Param, L, AL, K> {
        void onInvoke(LockHelper<Param, L, AL> locker, K lock);
    }

    private interface ICheckHolder<Param, Data> {
        DataPack<Data> getCheckedDataPack(DataParam<Param> param, Supplier<DataPack<Data>> supplier);

        default void updateNextCheckTime(DataParam<Param> param) {
        }
    }
}
