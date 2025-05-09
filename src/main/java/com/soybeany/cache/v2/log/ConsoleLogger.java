package com.soybeany.cache.v2.log;

/**
 * @author Soybeany
 * @date 2020/10/19
 */
public class ConsoleLogger<Param> extends StdLogger<Param> {
    public ConsoleLogger() {
        super(new ConsoleLogWriter());
    }
}
