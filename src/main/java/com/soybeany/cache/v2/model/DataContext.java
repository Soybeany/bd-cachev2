package com.soybeany.cache.v2.model;

import com.soybeany.cache.v2.contract.frame.ILogger;

import java.util.Optional;

public class DataContext {
    public final String dataDesc;
    public final String storageId;
    public final ILogger logger;

    public DataContext(String dataDesc, String storageId, ILogger logger) {
        this.dataDesc = dataDesc;
        this.storageId = storageId;
        this.logger = Optional.ofNullable(logger).orElseGet(NoLoggerImpl::new);
    }

    private static class NoLoggerImpl implements ILogger {
    }
}
