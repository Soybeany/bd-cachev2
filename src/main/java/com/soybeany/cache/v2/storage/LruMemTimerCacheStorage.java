package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.model.CacheEntity;
import com.soybeany.cache.v2.model.DataContext;
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

    public LruMemTimerCacheStorage(int pTtl, int pTtlErr, int capacity, boolean enableShareStorage) {
        super(pTtl, pTtlErr, capacity, enableShareStorage);
    }

    @Override
    public String desc() {
        return super.desc() + "_TIMER";
    }

    @Override
    protected synchronized CacheEntity<Data> onLoadCacheEntity(DataContext<Param> context, String key) throws NoCacheException {
        Task task = taskMap.get(key);
        if (null == task) {
            throw new NoCacheException();
        }
        task.uid = scheduleTask(context, key, task.pTtl);
        return super.onLoadCacheEntity(context, key);
    }

    @Override
    protected synchronized CacheEntity<Data> onSaveCacheEntity(DataContext<Param> context, String key, CacheEntity<Data> entity) {
        long pTtl = entity.pExpireAt - onGetCurTimestamp();
        String uid = scheduleTask(context, key, pTtl);
        taskMap.put(key, new Task(uid, pTtl));
        return super.onSaveCacheEntity(context, key, entity);
    }

    // ********************内部方法********************

    private String scheduleTask(DataContext<Param> context, String key, long pTtl) {
        String uid = BdFileUtils.getUuid();
        SERVICE.schedule(() -> removeData(context, key, uid), pTtl, TimeUnit.MILLISECONDS);
        return uid;
    }

    private synchronized void removeData(DataContext<Param> context, String key, String uid) {
        Task task = taskMap.get(key);
        if (null == task || !uid.equals(task.uid)) {
            return;
        }
        taskMap.remove(key);
        super.onRemoveCacheEntity(context, key);
    }

    // ********************内部类********************

    public static class Builder<Param, Data> extends LruMemCacheStorage.Builder<Param, Data> {
        @Override
        protected ICacheStorage<Param, Data> onBuild() {
            return new LruMemTimerCacheStorage<>(pTtl, pTtlErr, capacity, enableShareStorage);
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
