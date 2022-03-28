package com.soybeany.cache.v2.log;

/**
 * @author Soybeany
 * @date 2020/12/8
 */
public class ConsoleLogWriter implements ILogWriter {
    @Override
    public void onWriteInfo(String msg) {
        System.out.println(msg);
    }

    @Override
    public void onWriteWarn(String msg) {
        System.out.println(msg);
    }
}
