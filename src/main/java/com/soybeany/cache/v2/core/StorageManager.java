package com.soybeany.cache.v2.core;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.frame.IKeyLock;
import com.soybeany.cache.v2.contract.user.ICacheChecker;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.contract.user.IOnInvalidListener;
import com.soybeany.cache.v2.exception.CacheWaitException;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.exception.NoDataSourceException;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataCore;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.model.DataParam;
import com.soybeany.cache.v2.storage.StdKeyLock;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class StorageManager<Param, Data> {

    private static final ExecutorService DEFAULT_ASYNC_FETCH_EXECUTOR = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "bd-cache-afe");
        t.setDaemon(true);
        return t;
    });

    private final LinkedList<ICacheStorage<Param, Data>> storages = new LinkedList<>();
    private final Set<IOnInvalidListener<Param>> onInvalidListeners = new HashSet<>();

    private DataContext context;
    private ICheckHolder<Param, Data> checkerHolder = (param, supplier) -> supplier.get();
    private boolean enableRenewExpiredCache;
    private IKeyLock fetchLock = new StdKeyLock("fetch", k -> 30 * 1000L);
    private Function<String, Long> datasourceTimeoutSupplier;

    private ExecutorService asyncFetchExecutor = DEFAULT_ASYNC_FETCH_EXECUTOR;

    public DataPack<Data> getDataDirectly(Object noDatasourceInvoker, Param param, IDatasource<Param, Data> datasource, Long timeoutMs) {
        // 没有指定数据源
        if (null == datasource) {
            return new DataPack<>(DataCore.fromException(new NoDataSourceException()), noDatasourceInvoker, Long.MAX_VALUE);
        }
        // 同步模式（不开启异步数据源访问）
        if (null == timeoutMs) {
            try {
                Data data = datasource.onGetData(param);
                return new DataPack<>(DataCore.fromData(data), datasource, datasource.onSetupExpiry(param, data));
            } catch (RuntimeException e) {
                return new DataPack<>(DataCore.fromException(e), datasource, datasource.onSetupExpiry(param, e));
            }
        }
        // 异步执行+超时
        try {
            Future<Data> future = asyncFetchExecutor.submit(() -> datasource.onGetData(param));
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

    public void setAsyncDatasourceConfig(Function<String, Long> supplier) {
        this.datasourceTimeoutSupplier = supplier;
    }

    public Long getDatasourceTimeout(String paramKey) {
        return null != datasourceTimeoutSupplier ? datasourceTimeoutSupplier.apply(paramKey) : null;
    }

    public void setFetchLock(IKeyLock fetchLock) {
        this.fetchLock = fetchLock;
    }

    public void setAsyncFetchExecutor(ExecutorService asyncFetchExecutor) {
        this.asyncFetchExecutor = asyncFetchExecutor;
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

    /**
     * 获取数据并自动缓存(短超时回退模式)
     * <br>使用短超时访问数据源，超时后回退到过期缓存
     */
    public DataPack<Data> getDataPackWithCacheFallback(DataParam<Param> param, IDatasource<Param, Data> datasource, boolean needStore, long quickTimeoutMs, Function<DataPack<Data>, DataPack<Data>> fallbackProcessor) {
        // 1. 捕获子线程引用，用于fallback时精确中断
        Thread[] subThreadRef = new Thread[1];
        Future<DataPack<Data>> future = asyncFetchExecutor.submit(() -> {
            subThreadRef[0] = Thread.currentThread();
            return getDataPack(param, datasource, needStore);
        });
        // 2. 短超时等待
        try {
            return future.get(quickTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException | InterruptedException e) {
            if (null != subThreadRef[0]) {
                fetchLock.cancelIfWaiting(subThreadRef[0]);
            }
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

    public Map<Integer, Exception> cacheData(DataParam<Param> param, DataPack<Data> dataPack) {
        Map<Integer, Exception> result = traverseR((storage, prePack) -> storage.onCacheData(param, prePack), dataPack);
        checkerHolder.updateNextCheckTime(param);
        return result;
    }

    public Map<Integer, Exception> batchCacheData(Map<DataParam<Param>, DataPack<Data>> dataPacks) {
        Set<DataParam<Param>> params = dataPacks.keySet();
        Map<Integer, Exception> result = traverseR(ICacheStorage::onBatchCacheData, dataPacks);
        params.forEach(param -> checkerHolder.updateNextCheckTime(param));
        return result;
    }

    public Map<Integer, Exception> invalidCache(DataParam<Param> param, int... storageIndexes) {
        List<Integer> onSuccess = new ArrayList<>();
        Map<Integer, Exception> result = traverse((index, storage) -> {
            storage.onInvalidCache(param);
            onSuccess.add(index);
        }, storageIndexes);
        onInvalidListeners.forEach(listener -> listener.onInvoke(param.value, onSuccess, result));
        return result;
    }

    public Map<Integer, Exception> invalidAllCache(int... storageIndexes) {
        return traverse((index, storage) -> storage.onInvalidAllCache(), storageIndexes);
    }

    public Map<Integer, Exception> removeCache(DataParam<Param> param, int... storageIndexes) {
        return traverse((index, storage) -> storage.onRemoveCache(param), storageIndexes);
    }

    public Map<Integer, Exception> clearCache(int... storageIndexes) {
        return traverse((index, storage) -> storage.onClearCache(), storageIndexes);
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

    private <T> Map<Integer, Exception> traverseR(ICallback1<Param, Data, T> callback, T data) {
        Map<Integer, Exception> result = new HashMap<>();
        for (int i = storages.size() - 1; i >= 0; i--) {
            try {
                data = callback.onInvoke(storages.get(i), data);
            } catch (Exception e) {
                result.put(i, e);
            }
        }
        return result;
    }

    private Map<Integer, Exception> traverse(ICallback2<Param, Data> callback, int... storageIndexes) {
        Predicate<Integer> filter = i -> true;
        if (null != storageIndexes && storageIndexes.length > 0) {
            Set<Integer> indexes = Arrays.stream(storageIndexes).boxed().collect(Collectors.toSet());
            filter = indexes::contains;
        }
        Map<Integer, Exception> result = new HashMap<>();
        for (int i = 0; i < storages.size(); i++) {
            if (filter.test(i)) {
                try {
                    callback.onInvoke(i, storages.get(i));
                } catch (Exception e) {
                    result.put(i, e);
                }
            }
        }
        return result;
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
                dataPackHolder.add(getDataDirectly(this, param.value, datasource, getDatasourceTimeout(param.paramKey)));
                // 在fetch锁内回写所有缓存层，释放锁后其他线程可直接读到
                if (needStore) {
                    for (int i = storages.size() - 1; i >= 0; i--) {
                        ICacheStorage<Param, Data> s = storages.get(i);
                        onExe(() -> dataPackHolder.set(0, s.onCacheData(param, dataPackHolder.get(0))), onException);
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
                    dataPack = onExe(() -> storage.onCacheData(param, dp), onException);
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
            return storage.onCacheData(param, dataPack);
        }
    }

    private <T> T exeWithFetchLock(DataParam<Param> param, Supplier<T> callback, Function<RuntimeException, T> onException) {
        try {
            fetchLock.onTryLock(param.paramKey);
        } catch (RuntimeException e) {
            context.logger.onLockException(param.paramKey, e);
            return onException.apply(e);
        }
        try {
            return onExe(callback, onException);
        } finally {
            try {
                fetchLock.onUnlock(param.paramKey);
            } catch (RuntimeException e) {
                context.logger.onLockException(param.paramKey, e);
            }
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

    // ****************************************内部类****************************************

    private interface ICallback1<Param, Data, T> {
        T onInvoke(ICacheStorage<Param, Data> storage, T data);
    }

    private interface ICallback2<Param, Data> {
        void onInvoke(int index, ICacheStorage<Param, Data> storage);
    }

    private interface ICheckHolder<Param, Data> {
        DataPack<Data> getCheckedDataPack(DataParam<Param> param, Supplier<DataPack<Data>> supplier);

        default void updateNextCheckTime(DataParam<Param> param) {
        }
    }
}
