package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.frame.IKeyLock;
import com.soybeany.cache.v2.exception.CacheWaitException;

import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * 单锁支持（仅支持按key加锁，不支持全局锁）
 */
public class StdKeyLock implements IKeyLock<Lock> {
    public static final long LOCK_WAIT_TIME_DEFAULT = 30 * 1000;

    protected final String desc;
    protected final Map<String, Lock> lockMap = new WeakHashMap<>();
    protected final Lock mapLock = new ReentrantLock();
    protected final Function<String, Long> lockWaitTimeSupplier;

    public StdKeyLock(String desc, Function<String, Long> lockWaitTimeSupplier) {
        this.desc = desc;
        this.lockWaitTimeSupplier = Optional.ofNullable(lockWaitTimeSupplier).orElse(param -> LOCK_WAIT_TIME_DEFAULT);
    }

    @Override
    public Lock onTryLock(String key) {
        Lock lock = getLock(key);
        tryLock(key, lock, lockWaitTimeSupplier.apply(key));
        return lock;
    }

    @Override
    public void onUnlock(Lock lock) {
        lock.unlock();
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

    private Lock getLock(String key) {
        mapLock.lock();
        try {
            return lockMap.computeIfAbsent(key, k -> new ReentrantLock());
        } finally {
            mapLock.unlock();
        }
    }
}
