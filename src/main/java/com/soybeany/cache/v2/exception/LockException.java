package com.soybeany.cache.v2.exception;

public class LockException extends BdCacheException {
    public LockException(String msg) {
        super("锁异常:" + msg);
    }
}
