package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.model.CacheEntity;
import com.soybeany.cache.v2.model.DataParam;
import com.soybeany.util.file.BdFileUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 需在程序启动时调用{@link #createTimer}，程序结束时调用{@link #destroyTimer}
 *
 * @author Soybeany
 * @date 2022/2/9
 */
public class LruMemTimerCacheStorage<Param, Data> extends LruMemCacheStorage<Param, Data> {

    private static ScheduledExecutorService SERVICE;

    private final Map<String, Task> taskMap = new HashMap<>();

    @SuppressWarnings("AlibabaThreadPoolCreation")
    public synchronized static void createTimer() {
        if (null == SERVICE) {
            SERVICE = Executors.newScheduledThreadPool(1);
        }
    }

    public synchronized static void destroyTimer() {
        if (null != SERVICE) {
            SERVICE.shutdown();
            SERVICE = null;
        }
    }

    public LruMemTimerCacheStorage(long pTtl, long pTtlErr, int capacity) {
        super(pTtl, pTtlErr, capacity);
    }

    @Override
    public String desc() {
        return super.desc() + "_TIMER";
    }

    @Override
    protected synchronized CacheEntity<Data> onLoadCacheEntity(DataParam<Param> param, String storageKey) throws NoCacheException {
        Task task = taskMap.get(storageKey);
        if (null == task) {
            throw new NoCacheException();
        }
        task.uid = scheduleTask(param, storageKey, task.pTtl);
        return super.onLoadCacheEntity(param, storageKey);
    }

    @Override
    protected synchronized CacheEntity<Data> onSaveCacheEntity(DataParam<Param> param, String storageKey, CacheEntity<Data> entity) {
        long pTtl = entity.pExpireAt - onGetCurTimestamp();
        String uid = scheduleTask(param, storageKey, pTtl);
        taskMap.put(storageKey, new Task(uid, pTtl));
        return super.onSaveCacheEntity(param, storageKey, entity);
    }

    // ********************内部方法********************

    private String scheduleTask(DataParam<Param> param, String key, long pTtl) {
        String uid = BdFileUtils.getUuid();
        SERVICE.schedule(() -> removeData(param, key, uid), pTtl, TimeUnit.MILLISECONDS);
        return uid;
    }

    private synchronized void removeData(DataParam<Param> param, String key, String uid) {
        Task task = taskMap.get(key);
        if (null == task || !uid.equals(task.uid)) {
            return;
        }
        taskMap.remove(key);
        super.onRemoveCacheEntity(param, key);
    }

    // ********************内部类********************

    public static class Builder<Param, Data> extends LruMemCacheStorage.Builder<Param, Data> {
        @Override
        protected ICacheStorage<Param, Data> onBuild() {
            return new LruMemTimerCacheStorage<>(pTtl, pTtlErr, capacity);
        }
    }

    private static class Task {
        String uid;
        long pTtl;

        public Task(String uid, long pTtl) {
            this.uid = uid;
            this.pTtl = pTtl;
        }
    }
}
