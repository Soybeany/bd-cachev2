package com.soybeany.cache.v2.core;

import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.contract.IDatasource;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.exception.NoDataSourceException;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataCore;
import com.soybeany.cache.v2.model.DataPack;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 链式设计中的节点
 *
 * @author Soybeany
 * @date 2020/1/20
 */
@RequiredArgsConstructor
class CacheNode<Param, Data> {

    private final Map<String, Lock> mKeyMap = new WeakHashMap<>();
    @Getter
    private final ICacheStorage<Param, Data> curStorage;
    @Getter
    private final int priority;
    @Setter
    private CacheNode<Param, Data> nextNode;

    public static <Param, Data> DataPack<Data> getDataDirectly(Object invoker, Param param, IDatasource<Param, Data> datasource) {
        // 没有指定数据源
        if (null == datasource) {
            return new DataPack<>(DataCore.fromException(new NoDataSourceException()), invoker, Integer.MAX_VALUE);
        }
        // 正常执行
        try {
            Data data = datasource.onGetData(param);
            return new DataPack<>(DataCore.fromData(data), datasource, datasource.onSetupExpiry(data));
        } catch (Exception e) {
            return new DataPack<>(DataCore.fromException(e), datasource, datasource.onSetupExpiry(e));
        }
    }

    /**
     * 获取数据并自动缓存
     */
    public DataPack<Data> getDataPackAndAutoCache(DataContext<Param> context, final IDatasource<Param, Data> datasource) {
        return getDataFromCurNode(context, () -> {
            // 加锁，避免并发时数据重复获取
            Lock lock = getLock(context.paramKey);
            lock.lock();
            try {
                // 再查一次本节点，避免由于并发多次调用下一节点
                return getDataFromCurNode(context, () -> getDataFromNextNode(context, datasource));
            } finally {
                lock.unlock();
            }
        });
    }

    public void cacheData(DataContext<Param> context, DataPack<Data> pack) {
        traverse(node -> node.curStorage.onCacheData(context, pack));
    }

    public void removeCache(DataContext<Param> context, int... storageIndexes) {
        traverse(node -> node.curStorage.onRemoveCache(context), storageIndexes);
    }

    public void clearCache(String storageId, int... storageIndexes) {
        traverse(node -> node.curStorage.onClearCache(storageId), storageIndexes);
    }

    // ****************************************内部方法****************************************

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

    /**
     * 从下一节点获取数据
     */
    private DataPack<Data> getDataFromNextNode(DataContext<Param> context, IDatasource<Param, Data> datasource) {
        DataPack<Data> pack;
        // 若没有下一节点，则从数据源获取
        if (null == nextNode) {
            pack = getDataDirectly(curStorage, context.param, datasource);
        }
        // 否则从下一节点获取缓存
        else {
            pack = nextNode.getDataPackAndAutoCache(context, datasource);
        }
        return curStorage.onCacheData(context, pack);
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

}
