package com.soybeany.cache.v2.log;

/**
 * @author Soybeany
 * @date 2020/10/19
 */
public class ConsoleLogger extends StdLogger {
    public ConsoleLogger() {
        super(new ConsoleLogWriter());
    }
}
