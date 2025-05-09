package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.frame.ILockSupport;
import com.soybeany.cache.v2.exception.CacheWaitException;
import com.soybeany.cache.v2.model.DataParam;

import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class ReentrantLockSupport<Param> implements ILockSupport<Param, Lock, Object> {
    public static final long LOCK_WAIT_TIME_DEFAULT = 30 * 1000;

    private final String desc;
    private final Lock lock = new ReentrantLock();
    private final Map<String, Lock> mKeyMap = new WeakHashMap<>();
    private final Function<Param, Long> lockWaitTimeSingleSupplier;
    private final long lockWaitTimeAll;

    public ReentrantLockSupport(String desc) {
        this(desc, null, null);
    }

    public ReentrantLockSupport(String desc, Function<Param, Long> lockWaitTimeSingleSupplier, Long lockWaitTimeAll) {
        this.desc = desc;
        this.lockWaitTimeSingleSupplier = Optional.ofNullable(lockWaitTimeSingleSupplier).orElse(param -> LOCK_WAIT_TIME_DEFAULT);
        this.lockWaitTimeAll = Optional.ofNullable(lockWaitTimeAll).orElse(LOCK_WAIT_TIME_DEFAULT);
    }

    @Override
    public Lock onTryLock(DataParam<Param> param) {
        Lock lock = getLock(param);
        tryLock(param.paramDesc, lock, lockWaitTimeSingleSupplier.apply(param.value));
        return lock;
    }

    @Override
    public void onUnlock(Lock lock) {
        lock.unlock();
    }

    @Override
    public Object onTryLockAll() {
        tryLock("全局", lock, lockWaitTimeAll);
        return null;
    }

    @Override
    public void onUnlockAll(Object l) {
        onUnlock(lock);
    }

    // ***********************内部方法****************************

    private void tryLock(String descDetail, Lock lock, long lockWaitTime) {
        try {
            if (!lock.tryLock(lockWaitTime, TimeUnit.MILLISECONDS)) {
                throw new CacheWaitException("锁超时(" + desc + "-" + descDetail + ")");
            }
        } catch (InterruptedException e) {
            throw new CacheWaitException("锁中断(" + desc + "-" + descDetail + ")");
        }
    }

    private Lock getLock(DataParam<Param> param) {
        Object allLock = onTryLockAll();
        try {
            return mKeyMap.computeIfAbsent(param.paramKey, k -> new ReentrantLock());
        } finally {
            onUnlockAll(allLock);
        }
    }
}
