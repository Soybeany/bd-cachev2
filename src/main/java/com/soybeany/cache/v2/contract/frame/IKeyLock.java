package com.soybeany.cache.v2.contract.frame;


public interface IKeyLock {

    void onTryLock(String key);

    void onUnlock(String key);

    void cancelIfWaiting(Thread thread);

}
