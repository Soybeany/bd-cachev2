package com.soybeany.cache.v2.contract.frame;

import com.soybeany.cache.v2.model.DataParam;

public interface ILockSupport<Param, L, AL> {

    L onTryLock(DataParam<Param> param);

    void onUnlock(L lock);

    AL onTryLockAll();

    void onUnlockAll(AL lock);

}
