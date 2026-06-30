package com.soybeany.cache.v2.core;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.frame.ILockSupport;
import com.soybeany.cache.v2.contract.user.ICacheChecker;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.contract.user.IOnInvalidListener;
import com.soybeany.cache.v2.exception.CacheWaitException;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.exception.NoDataSourceException;
import com.soybeany.cache.v2.model.*;
import com.soybeany.cache.v2.contract.frame.IKeyLock;
import com.soybeany.cache.v2.storage.KeyLock;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
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
    private IKeyLock<Lock> fetchLockSupport;
    private Function<String, Long> fetchLockTimeoutSingleSupplier;

    private long datasourceTimeout = 30 * 1000;

    static final long DEFAULT_QUICK_TIMEOUT_MS = 2000L;
    private static final ExecutorService DATASOURCE_EXECUTOR = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "bd-cache-ds");
        t.setDaemon(true);
        return t;
    });

    public static <Param, Data> DataPack<Data> getDataDirectly(Object noDatasourceInvoker, Param param, IDatasource<Param, Data> datasource, long timeoutMs) {
        // 没有指定数据源
        if (null == datasource) {
            return new DataPack<>(DataCore.fromException(new NoDataSourceException()), noDatasourceInvoker, Long.MAX_VALUE);
        }
        // 异步执行+超时
        try {
            Future<Data> future = DATASOURCE_EXECUTOR.submit(() -> datasource.onGetData(param));
            Data data;
            try {
                data = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
                throw new CacheWaitException("数据源访问超时");
            } catch (InterruptedException e) {
                future.cancel(true);
                throw new CacheWaitException("数据源访问中断");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException(cause);
            }
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

    public void setDatasourceTimeout(long datasourceTimeout) {
        this.datasourceTimeout = datasourceTimeout;
    }

    public long getDatasourceTimeout() {
        return datasourceTimeout;
    }

    public void setFetchLockTimeoutSingleSupplier(Function<String, Long> fetchLockTimeoutSingleSupplier) {
        this.fetchLockTimeoutSingleSupplier = fetchLockTimeoutSingleSupplier;
    }

    public void init(DataContext context) {
        this.context = context;
        fetchLockSupport = new KeyLock("fetch", fetchLockTimeoutSingleSupplier);
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

    /**
     * 获取数据并自动缓存(短超时回退模式)
     * <br>使用短超时访问数据源，超时后回退到过期缓存
     */
    public DataPack<Data> getDataPackWithCacheFallback(DataParam<Param> param, IDatasource<Param, Data> datasource, boolean needStore, long quickTimeoutMs, Function<DataPack<Data>, DataPack<Data>> fallbackProcessor) {
        // 1. 异步获取新数据(完整超时)
        Future<DataPack<Data>> future = DATASOURCE_EXECUTOR.submit(() -> getDataPack(param, datasource, needStore));
        // 2. 短超时等待
        try {
            return future.get(quickTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException | InterruptedException e) {
            return fallbackProcessor.apply(onGetCacheDataPack(0, param));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            RuntimeException ex = cause instanceof RuntimeException ? (RuntimeException) cause : new RuntimeException(cause);
            return fallbackProcessor.apply(new DataPack<>(DataCore.fromException(ex), this, 0));
        }
    }

    /**
     * 获取当前缓存（即使已过期），不访问数据源
     */
    public DataPack<Data> getCacheDataPack(DataParam<Param> param) {
        return onGetCacheDataPack(0, param);
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
        DataPack<Data> dataPack = onGetCacheDataPack(0, param);
        if (dataPack.dataCore.exception instanceof NoCacheException) {
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
            return exeWithFetchLock(param, () -> {
                // 双重检查：其他线程可能已写入缓存
                for (ICacheStorage<Param, Data> storage : storages) {
                    try {
                        return storage.onGetCache(param);
                    } catch (NoCacheException ignored) {
                    }
                }
                List<DataPack<Data>> dataPackHolder = new ArrayList<>();
                dataPackHolder.add(getDataDirectly(this, param.value, datasource, datasourceTimeout));
                // 在fetch锁内回写所有缓存层，释放锁后其他线程可直接读到
                if (needStore) {
                    for (int i = storages.size() - 1; i >= 0; i--) {
                        ICacheStorage<Param, Data> s = storages.get(i);
                        exeWithLock(s, param, () -> dataPackHolder.set(0, s.onCacheData(param, dataPackHolder.get(0))), onException);
                    }
                }
                return dataPackHolder.get(0);
            }, onException);
        }

        ICacheStorage<Param, Data> storage = storages.get(storageIndex);
        return onExe(() -> {
            try {
                // 从当前storage获取数据
                return storage.onGetCache(param);
            } catch (NoCacheException e) {
                // 从下一storage获取数据
                DataPack<Data> dataPack = onGetDataPack(storageIndex + 1, param, datasource, needStore, onException);
                // 按需缓存数据（数据提升：非数据源路径时才回写，数据源路径已由边界统一回写）
                if (needStore && !isFromDatasource(dataPack)) {
                    DataPack<Data> dp = dataPack;
                    dataPack = exeWithLock(storage, param, () -> storage.onCacheData(param, dp), onException);
                }
                return dataPack;
            }
        }, onException);
    }

    private DataPack<Data> onGetCacheDataPack(int storageIndex, DataParam<Param> param) {
        if (storageIndex >= storages.size()) {
            return new DataPack<>(DataCore.fromException(new NoCacheException()), this, 0);
        }
        ICacheStorage<Param, Data> storage = storages.get(storageIndex);
        try {
            return storage.onGetCacheIgnoreExpiry(param);
        } catch (NoCacheException e) {
            DataPack<Data> dataPack = onGetCacheDataPack(storageIndex + 1, param);
            return exeWithLock(storage, param, () -> storage.onCacheData(param, dataPack));
        }
    }

    private void exeWithLock(ICacheStorage<Param, Data> storage, DataParam<Param> param, Runnable callback) {
        exeWithLock(storage, param, toFunc(callback));
    }

    private <T> T exeWithLock(ICacheStorage<Param, Data> storage, DataParam<Param> param, Supplier<T> callback) {
        return exeWithLock(storage, param, callback, getOnException());
    }

    private <T> T exeWithLock(ICacheStorage<Param, Data> storage, DataParam<Param> param, Supplier<T> callback, Function<RuntimeException, T> onException) {
        String key = param.paramKey;
        return onExeWithLockAuto(storage, lockHelper -> lockHelper.tryLock(key), (lockHelper, lock) -> lockHelper.unlock(key, lock), callback, onException);
    }

    private <T> T exeWithLockBatch(ICacheStorage<Param, Data> storage, Collection<DataParam<Param>> params, Supplier<T> callback) {
        Collection<String> keys = params.stream().map(p -> p.paramKey).collect(Collectors.toList());
        return onExeWithLockAuto(storage, lockHelper -> lockHelper.tryLockBatch(keys), LockHelper::unlockBatch, callback, getOnException());
    }

    private void exeWithLockAll(ICacheStorage<Param, Data> storage, Runnable callback) {
        onExeWithLockAuto(storage, LockHelper::tryLockAll, LockHelper::unlockAll, toFunc(callback), getOnException());
    }

    private <T> T exeWithFetchLock(DataParam<Param> param, Supplier<T> callback, Function<RuntimeException, T> onException) {
        Lock lock;
        try {
            lock = fetchLockSupport.onTryLock(param.paramKey);
        } catch (RuntimeException e) {
            context.logger.onLockException(param.paramKey, e);
            return onException.apply(e);
        }
        try {
            return onExe(callback, onException);
        } finally {
            try {
                fetchLockSupport.onUnlock(lock);
            } catch (RuntimeException e) {
                context.logger.onLockException(param.paramKey, e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <L, AL, T, K> T onExeWithLockAuto(ICacheStorage<Param, Data> storage, ICallback3<L, AL, K> onLock, ICallback4<L, AL, K> onUnlock, Supplier<T> callback, Function<RuntimeException, T> onException) {
        if (storage instanceof ILockSupport) {
            LockHelper<L, AL> lockHelper = new LockHelper<>(context, (ILockSupport<L, AL>) storage);
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

    private interface ICallback3<L, AL, K> {
        K onInvoke(LockHelper<L, AL> locker);
    }

    private interface ICallback4<L, AL, K> {
        void onInvoke(LockHelper<L, AL> locker, K lock);
    }

    private interface ICheckHolder<Param, Data> {
        DataPack<Data> getCheckedDataPack(DataParam<Param> param, Supplier<DataPack<Data>> supplier);

        default void updateNextCheckTime(DataParam<Param> param) {
        }
    }
}
