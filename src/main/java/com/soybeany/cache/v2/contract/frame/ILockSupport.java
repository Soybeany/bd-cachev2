package com.soybeany.cache.v2.contract.frame;


public interface ILockSupport<L, AL> extends ISingleLockSupport<L> {

    AL onTryLockAll();

    void onUnlockAll(AL lock);

}
