package com.soybeany.cache.v2.contract.frame;


public interface ISingleLockSupport<L> {

    L onTryLock(String key);

    void onUnlock(L lock);

}
