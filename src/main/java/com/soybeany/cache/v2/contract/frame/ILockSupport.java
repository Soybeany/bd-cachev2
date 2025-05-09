package com.soybeany.cache.v2.contract.frame;


public interface ILockSupport<L, AL> {

    L onTryLock(String key);

    void onUnlock(L lock);

    AL onTryLockAll();

    void onUnlockAll(AL lock);

}
