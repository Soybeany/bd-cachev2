package com.soybeany.cache.v2.exception;

public class CacheWaitException extends BdCacheException {
    public CacheWaitException(String msg) {
        super("缓存等待异常:" + msg);
    }
}
