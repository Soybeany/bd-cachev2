package com.soybeany.cache.v2.core;

import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.contract.IDataChecker;
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

/**
 * 链式设计中的节点
 *
 * @author Soybeany
 * @date 2020/1/20
 */
class CacheNode<Param, Data> {

    private final Map<String, Lock> mKeyMap = new WeakHashMap<>();
    private final ICacheStorage<Param, Data> curStorage;
    private final long lockWaitTime;

    private int index;
    private CacheNode<Param, Data> nextNode;
    private IDataChecker.Holder<Param, Data> checkerHolder;

    public static <Param, Data> DataPack<Data> getDataDirectly(Object invoker, Param param, IDatasource<Param, Data> datasource) {
        // 没有指定数据源
        if (null == datasource) {
            return new DataPack<>(DataCore.fromException(new NoDataSourceException()), invoker, Long.MAX_VALUE);
        }
        // 正常执行
        try {
            Data data = datasource.onGetData(param);
            return new DataPack<>(DataCore.fromData(data), datasource, datasource.onSetupExpiry(data));
        } catch (RuntimeException e) {
            return new DataPack<>(DataCore.fromException(e), datasource, datasource.onSetupExpiry(e));
        }
    }

    public CacheNode(ICacheStorage<Param, Data> curStorage, long lockWaitTime) {
        this.curStorage = curStorage;
        this.lockWaitTime = lockWaitTime;
    }

    public ICacheStorage<Param, Data> getCurStorage() {
        return curStorage;
    }

    public CacheNode<Param, Data> getNextNode() {
        return nextNode;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public void setNextNode(CacheNode<Param, Data> nextNode) {
        this.nextNode = nextNode;
    }

    public void setCheckerHolder(IDataChecker.Holder<Param, Data> checkerHolder) {
        this.checkerHolder = checkerHolder;
    }

    /**
     * 获取数据并自动缓存
     */
    public DataPack<Data> getDataPack(DataContext<Param> context, IDatasource<Param, Data> datasource, boolean needStore) {
        return exeWithLock(context, useLock -> {
            DataPack<Data> pack = getDataFromCurNodeOrNext(context, datasource, needStore);
            return useLock ? getDataWithCheck(context, pack, datasource, needStore) : pack;
        }, e -> new DataPack<>(DataCore.fromException(e), curStorage, 0));
    }

    public void cacheData(DataContext<Param> context, DataPack<Data> pack) {
        exeWithLock(context, () -> {
            traverseR((node, prePack) -> node.curStorage.onCacheData(context, prePack), pack);
            setupNextCheckStamp(context);
        });
    }

    public void batchCacheData(DataContext.Core<Param, Data> contextCore, Map<DataContext.Param<Param>, DataPack<Data>> dataPacks) {
        traverseR((node, prePacks) -> node.curStorage.onBatchCacheData(contextCore, prePacks), dataPacks);
        dataPacks.forEach((k, v) -> setupNextCheckStamp(new DataContext<>(contextCore, k)));
    }

    public void invalidCache(DataContext<Param> context, int... storageIndexes) {
        traverse(node -> node.curStorage.onInvalidCache(context), storageIndexes);
    }

    public void invalidAllCache(DataContext.Core<Param, Data> contextCore, int... storageIndexes) {
        traverse(node -> node.curStorage.onInvalidAllCache(contextCore), storageIndexes);
    }

    public void removeCache(DataContext<Param> context, int... storageIndexes) {
        traverse(node -> node.curStorage.onRemoveCache(context), storageIndexes);
    }

    public void clearCache(DataContext.Core<Param, Data> contextCore, int... storageIndexes) {
        traverse(node -> node.curStorage.onClearCache(contextCore), storageIndexes);
    }

    public void dataCheck(DataContext<Param> context, IDataChecker<Param, Data> checker) {
        boolean needUpdate = checker.needUpdate(context.param.param, pack);
        context.core.logger.onCheckCache(context, needUpdate);
        // 若检查结果为需要更新，则先失效缓存，再获取一次数据，最后设置检查时间戳
        if (needUpdate) {
            invalidCache(context);
            pack = getDataFromCurNodeOrNext(context, datasource, needStore);
            setupNextCheckStamp(context);
        }
    }

    // ****************************************内部方法****************************************

    private <T> void traverseR(ICallback4<Param, Data, T> callback, T previous) {
        List<CacheNode<Param, Data>> nodes = new ArrayList<>();
        CacheNode<Param, Data> node = this;
        // 得到正向列表
        while (null != node) {
            nodes.add(node);
            node = node.nextNode;
        }
        // 反向遍历
        for (int i = nodes.size() - 1; i >= 0; i--) {
            previous = callback.onInvoke(nodes.get(i), previous);
        }
    }

    private void traverse(ICallback2<Param, Data> callback, int... storageIndexes) {
        ICallback3 callback3 = getCallback3(storageIndexes);
        CacheNode<Param, Data> node = this;
        int index = 0;
        while (null != node) {
            if (callback3.shouldInvoke(index++)) {
                callback.onInvoke(node);
            }
            node = node.nextNode;
        }
    }

    private DataPack<Data> getDataWithCheck(DataContext<Param> context, DataPack<Data> pack, IDatasource<Param, Data> datasource, boolean needStore) {
        // 不需要数据检查
        if (null == checkerHolder) {
            return pack;
        }
        // 若来源于数据源，则直接赋值
        if (pack.provider instanceof IDatasource) {
            setupNextCheckStamp(context);
        }
        // 若晚于检查时间戳，则检查
        else if (System.currentTimeMillis() > curStorage.getNextCheckStamp(context)) {
            boolean needUpdate = checkerHolder.checker.needUpdate(context.param.param, pack);
            context.core.logger.onCheckCache(context, needUpdate);
            // 若检查结果为需要更新，则先失效缓存，再获取一次数据，最后设置检查时间戳
            if (needUpdate) {
                invalidCache(context);
                pack = getDataFromCurNodeOrNext(context, datasource, needStore);
                setupNextCheckStamp(context);
            }
        }
        return pack;
    }

    private void exeWithLock(DataContext<Param> context, Consumer<Boolean> callback) {
        exeWithLock(context, useLock -> {
            callback.accept(useLock);
            return null;
        }, e -> {
            throw new BdCacheException(e.getMessage());
        });
    }

    private <T> T exeWithLock(DataContext<Param> context, Function<Boolean, T> callback, Function<RuntimeException, T> onException) {
        // 若非第一节点，不需要加锁
        if (index != 0) {
            try {
                return callback.apply(false);
            } catch (RuntimeException e) {
                return onException.apply(e);
            }
        }
        // 加锁，避免并发时数据重复获取
        Lock lock = getLock(context.param.paramKey);
        try {
            if (!lock.tryLock(lockWaitTime, TimeUnit.SECONDS)) {
                return onException.apply(new CacheWaitException("超时"));
            }
        } catch (InterruptedException e) {
            return onException.apply(new CacheWaitException("中断"));
        }
        try {
            return callback.apply(true);
        } catch (RuntimeException e) {
            return onException.apply(e);
        } finally {
            lock.unlock();
        }
    }

    private void setupNextCheckStamp(DataContext<Param> context) {
        curStorage.setNextCheckStamp(context, System.currentTimeMillis() + checkerHolder.minInterval);
    }

    /**
     * 从下一节点获取数据
     */
    private DataPack<Data> getDataFromNextNode(DataContext<Param> context, IDatasource<Param, Data> datasource, boolean needStore) {
        DataPack<Data> pack;
        // 若没有下一节点，则从数据源获取
        if (null == nextNode) {
            pack = getDataDirectly(curStorage, context.param.param, datasource);
        }
        // 否则从下一节点获取缓存
        else {
            pack = nextNode.getDataPack(context, datasource, needStore);
        }
        return needStore ? curStorage.onCacheData(context, pack) : pack;
    }

    private DataPack<Data> getDataFromCurNodeOrNext(DataContext<Param> context, IDatasource<Param, Data> datasource, boolean needStore) {
        return getDataFromCurNodeOrCallback(context, () -> getDataFromNextNode(context, datasource, needStore));
    }

    /**
     * 从本地缓存服务获取数据
     */
    private DataPack<Data> getDataFromCurNodeOrCallback(DataContext<Param> context, ICallback1<Data> callback) {
        try {
            return curStorage.onGetCache(context);
        } catch (NoCacheException e) {
            return callback.onNoCache();
        }
    }

    private ICallback3 getCallback3(int... storageIndexes) {
        if (null == storageIndexes || 0 == storageIndexes.length) {
            return index -> true;
        }
        Set<Integer> indexSet = new HashSet<>();
        for (int index : storageIndexes) {
            indexSet.add(index);
        }
        return indexSet::contains;
    }

    private Lock getLock(String key) {
        synchronized (mKeyMap) {
            return mKeyMap.computeIfAbsent(key, k -> new ReentrantLock());
        }
    }

    // ****************************************内部类****************************************

    private interface ICallback1<Data> {
        DataPack<Data> onNoCache();
    }

    private interface ICallback2<Param, Data> {
        void onInvoke(CacheNode<Param, Data> node);
    }

    private interface ICallback3 {
        boolean shouldInvoke(int index);
    }

    private interface ICallback4<Param, Data, T> {
        T onInvoke(CacheNode<Param, Data> node, T previous);
    }

}
