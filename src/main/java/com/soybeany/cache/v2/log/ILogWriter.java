package com.soybeany.cache.v2.log;

/**
 * @author Soybeany
 * @date 2020/12/8
 */
public interface ILogWriter {

    void onWriteInfo(String msg);

    void onWriteWarn(String msg);

}
