package com.soybeany.cache.v2.exception;

/**
 * 数据源相关的异常
 *
 * @author Soybeany
 * @date 2020/10/9
 */
public class NoDataSourceException extends BdCacheException {
    public NoDataSourceException() {
        super("没有数据源");
    }
}
