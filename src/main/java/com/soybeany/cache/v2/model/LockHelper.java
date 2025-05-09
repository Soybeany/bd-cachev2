package com.soybeany.cache.v2.model;

import com.soybeany.cache.v2.contract.frame.ILockSupport;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class LockHelper<L, AL> {

    private final DataContext context;
    private final ILockSupport<L, AL> lockSupport;

    public LockHelper(DataContext context, ILockSupport<L, AL> lockSupport) {
        this.context = context;
        this.lockSupport = lockSupport;
    }

    public L tryLock(String key) {
        try {
            return lockSupport.onTryLock(key);
        } catch (RuntimeException e) {
            context.logger.onLockException(key, e);
            throw e;
        }
    }

    public void unlock(String key, L lock) {
        try {
            lockSupport.onUnlock(lock);
        } catch (RuntimeException e) {
            context.logger.onLockException(key, e);
        }
    }

    public Map<String, L> tryLockBatch(Collection<String> keys) {
        Map<String, L> locking = new HashMap<>();
        for (String key : keys) {
            try {
                L lock = tryLock(key);
                locking.put(key, lock);
            } catch (RuntimeException e) {
                unlockBatch(locking);
                throw e;
            }
        }
        return locking;
    }

    public void unlockBatch(Map<String, L> locking) {
        locking.forEach(this::unlock);
    }

    public AL tryLockAll() {
        try {
            return lockSupport.onTryLockAll();
        } catch (RuntimeException e) {
            context.logger.onLockException(null, e);
            throw e;
        }
    }

    public void unlockAll(AL lock) {
        try {
            lockSupport.onUnlockAll(lock);
        } catch (RuntimeException e) {
            context.logger.onLockException(null, e);
        }
    }

}
