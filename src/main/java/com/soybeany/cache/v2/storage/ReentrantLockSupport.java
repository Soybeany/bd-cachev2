package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.frame.ILockSupport;
import com.soybeany.cache.v2.exception.CacheWaitException;
import com.soybeany.cache.v2.model.DataParam;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ReentrantLockSupport<Param> implements ILockSupport<Param, Lock> {
    private final Lock lock = new ReentrantLock();
    private final Map<String, Lock> mKeyMap = new WeakHashMap<>();

    @Override
    public Lock onTryLock(DataParam<Param> param, long lockWaitTime) {
        Lock lock = getLock(param);
        tryLock(lock, lockWaitTime);
        return lock;
    }

    @Override
    public void onUnlock(Lock lock) {
        lock.unlock();
    }

    @Override
    public void onTryLockAll(long lockWaitTime) {
        tryLock(lock, lockWaitTime);
    }

    @Override
    public void onUnlockAll() {
        onUnlock(lock);
    }

    // ***********************内部方法****************************

    private void tryLock(Lock lock, long lockWaitTime) {
        try {
            if (!lock.tryLock(lockWaitTime, TimeUnit.SECONDS)) {
                throw new CacheWaitException("超时");
            }
        } catch (InterruptedException e) {
            throw new CacheWaitException("中断");
        }
    }

    private Lock getLock(DataParam<Param> param) {
        lock.lock();
        try {
            return mKeyMap.computeIfAbsent(param.paramKey, k -> new ReentrantLock());
        } finally {
            lock.unlock();
        }
    }
}
