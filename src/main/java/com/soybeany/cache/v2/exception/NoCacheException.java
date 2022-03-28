package com.soybeany.cache.v2.exception;

/**
 * 该异常只供框架内部使用，用户不应接触该异常
 * <br>Created by Soybeany on 2020/10/15.
 */
public class NoCacheException extends BdCacheException {
    public NoCacheException() {
        super("没有找到缓存");
    }
}
