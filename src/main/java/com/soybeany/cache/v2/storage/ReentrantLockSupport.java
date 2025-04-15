package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.exception.CacheWaitException;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ReentrantLockSupport implements ICacheStorage.ILockSupport<Lock, ReentrantLockSupport.LocksHolder> {
    private final Map<String, Lock> mKeyMap = new WeakHashMap<>();

    @Override
    public Lock onTryLock(String key, long lockWaitTime) {
        Lock lock = getLock(key);
        tryLock(lock, lockWaitTime);
        return lock;
    }

    @Override
    public void onUnlock(Lock lock) {
        lock.unlock();
    }

    @Override
    public LocksHolder onBatchLock(Collection<String> keys, long lockWaitTime) {
        LocksHolder holder = new LocksHolder(keys.stream().map(this::getLock).collect(Collectors.toList()));
        for (Lock lock : holder.locks) {
            tryLock(lock, lockWaitTime);
            holder.locking.add(lock);
        }
        return holder;
    }

    @Override
    public void onBatchUnlock(LocksHolder lockHolder) {
        for (Lock lock : lockHolder.locking) {
            try {
                lock.unlock();
            } catch (Exception e) {

            }
        }
    }

    // ***********************内部方法****************************

    private Lock tryLock(Lock lock, long lockWaitTime) {
        try {
            if (!lock.tryLock(lockWaitTime, TimeUnit.SECONDS)) {
                throw new CacheWaitException("超时");
            }
        } catch (InterruptedException e) {
            throw new CacheWaitException("中断");
        }
        return lock;
    }

    private Lock getLock(String key) {
        synchronized (mKeyMap) {
            return mKeyMap.computeIfAbsent(key, k -> new ReentrantLock());
        }
    }

    // ***********************内部类****************************

    public static class LocksHolder {
        final Collection<Lock> locks;
        public final List<Lock> locking = new ArrayList<>();

        LocksHolder(List<Lock> locks) {
            this.locks = locks;
        }
    }

}
