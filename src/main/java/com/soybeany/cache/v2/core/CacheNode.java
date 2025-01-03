package com.soybeany.cache.v2.core;

import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.contract.IDatasource;
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

/**
 * 链式设计中的节点
 *
 * @author Soybeany
 * @date 2020/1/20
 */
class CacheNode<Param, Data> {

    private final Map<String, Lock> mKeyMap = new WeakHashMap<>();
    private final ICacheStorage<Param, Data> curStorage;
    private final int priority;
    private final long lockWaitTime;
    private CacheNode<Param, Data> nextNode;

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

    public CacheNode(ICacheStorage<Param, Data> curStorage, int priority, long lockWaitTime) {
        this.curStorage = curStorage;
        this.priority = priority;
        this.lockWaitTime = lockWaitTime;
    }

    public ICacheStorage<Param, Data> getCurStorage() {
        return curStorage;
    }

    public int getPriority() {
        return priority;
    }

    public CacheNode<Param, Data> getNextNode() {
        return nextNode;
    }

    public void setNextNode(CacheNode<Param, Data> nextNode) {
        this.nextNode = nextNode;
    }

    /**
     * 获取数据并自动缓存
     */
    public DataPack<Data> getDataPack(DataContext<Param> context, final IDatasource<Param, Data> datasource, boolean needStore) {
        if (curStorage.needDoubleCheck()) {
            return getDataFromCurNode(context, () -> doGetDataPack(context, datasource, needStore));
        } else {
            return doGetDataPack(context, datasource, needStore);
        }
    }

    public void cacheData(DataContext<Param> context, DataPack<Data> pack) {
        traverseR((node, prePack) -> node.curStorage.onCacheData(context, prePack), pack);
    }

    public void batchCacheData(DataContext.Core<Param, Data> contextCore, Map<DataContext.Param<Param>, DataPack<Data>> dataPacks) {
        traverseR((node, prePacks) -> node.curStorage.onBatchCacheData(contextCore, prePacks), dataPacks);
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

    private DataPack<Data> doGetDataPack(DataContext<Param> context, final IDatasource<Param, Data> datasource, boolean needStore) {
        // 加锁，避免并发时数据重复获取
        Lock lock = getLock(context.param.paramKey);
        try {
            if (!lock.tryLock(lockWaitTime, TimeUnit.SECONDS)) {
                return new DataPack<>(DataCore.fromException(new CacheWaitException("超时")), curStorage, 0);
            }
        } catch (InterruptedException e) {
            return new DataPack<>(DataCore.fromException(new CacheWaitException("中断")), curStorage, 0);
        }
        try {
            // 再查一次本节点，避免由于并发多次调用下一节点
            return getDataFromCurNode(context, () -> getDataFromNextNode(context, datasource, needStore));
        } finally {
            lock.unlock();
        }
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

    /**
     * 从本地缓存服务获取数据
     */
    private DataPack<Data> getDataFromCurNode(DataContext<Param> context, ICallback1<Data> callback) {
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
