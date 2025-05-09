package com.soybeany.cache.v2.model;

import com.soybeany.cache.v2.contract.frame.ILockSupport;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class LockHelper<Param, L, AL> {

    private final DataContext context;
    private final ILockSupport<Param, L, AL> lockSupport;

    public LockHelper(DataContext context, ILockSupport<Param, L, AL> lockSupport) {
        this.context = context;
        this.lockSupport = lockSupport;
    }

    public L tryLock(DataParam<Param> param) {
        try {
            return lockSupport.onTryLock(param);
        } catch (RuntimeException e) {
            context.logger.onLockException(param, e);
            throw e;
        }
    }

    public void unlock(DataParam<Param> param, L lock) {
        try {
            lockSupport.onUnlock(lock);
        } catch (RuntimeException e) {
            context.logger.onLockException(param, e);
        }
    }

    public Map<DataParam<Param>, L> tryLockBatch(Collection<DataParam<Param>> params) {
        Map<DataParam<Param>, L> locking = new HashMap<>();
        for (DataParam<Param> param : params) {
            try {
                L lock = tryLock(param);
                locking.put(param, lock);
            } catch (RuntimeException e) {
                unlockBatch(locking);
                throw e;
            }
        }
        return locking;
    }

    public void unlockBatch(Map<DataParam<Param>, L> locking) {
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
