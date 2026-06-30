package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.frame.ILockSupport;

import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * 多锁支持（同时支持按key加锁和全局锁）
 */
public class ReentrantLockSupport extends SingleLockSupport implements ILockSupport<Lock, Object> {

    private final Lock allLock = new ReentrantLock();
    private final long lockWaitTimeAll;

    public ReentrantLockSupport(String desc) {
        this(desc, null, null);
    }

    public ReentrantLockSupport(String desc, Function<String, Long> lockWaitTimeSingleSupplier, Long lockWaitTimeAll) {
        super(desc, lockWaitTimeSingleSupplier);
        this.lockWaitTimeAll = Optional.ofNullable(lockWaitTimeAll).orElse(LOCK_WAIT_TIME_DEFAULT);
    }

    @Override
    public Object onTryLockAll() {
        tryLock("全局", allLock, lockWaitTimeAll);
        return null;
    }

    @Override
    public void onUnlockAll(Object l) {
        allLock.unlock();
    }

}
