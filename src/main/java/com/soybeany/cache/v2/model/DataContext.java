package com.soybeany.cache.v2.model;

import com.soybeany.cache.v2.contract.frame.ILogger;
import com.soybeany.cache.v2.contract.user.IKeyConverter;

public class DataContext<Param> {
    public final String dataDesc;
    public final String storageId;
    public final ILogger<Param> logger;

    private final IKeyConverter<Param> paramDescConverter;
    private final IKeyConverter<Param> paramKeyConverter;

    public DataContext(String dataDesc, String storageId, ILogger<Param> logger, IKeyConverter<Param> paramDescConverter, IKeyConverter<Param> paramKeyConverter) {
        this.dataDesc = dataDesc;
        this.storageId = storageId;
        this.logger = logger;
        this.paramDescConverter = paramDescConverter;
        this.paramKeyConverter = paramKeyConverter;
    }

    public DataParam<Param> toDataParam(Param param) {
        String paramDesc = paramDescConverter.getKey(param);
        String paramKey = paramKeyConverter.getKey(param);
        return new DataParam<>(paramDesc, paramKey, param);
    }

}
