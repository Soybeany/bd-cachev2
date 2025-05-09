package com.soybeany.cache.v2.model;

import com.soybeany.cache.v2.contract.frame.ILogger;

public class DataContext {
    public final String dataDesc;
    public final String storageId;
    public final ILogger logger;

    public DataContext(String dataDesc, String storageId, ILogger logger) {
        this.dataDesc = dataDesc;
        this.storageId = storageId;
        this.logger = logger;
    }
}
