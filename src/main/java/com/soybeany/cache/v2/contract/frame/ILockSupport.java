package com.soybeany.cache.v2.contract.frame;

import com.soybeany.cache.v2.model.DataParam;

public interface ILockSupport<Param, L> {
    L onTryLock(DataParam<Param> param, long lockWaitTime);

    void onUnlock(L lock);

    void onTryLockAll(long lockWaitTime);

    void onUnlockAll();
}
