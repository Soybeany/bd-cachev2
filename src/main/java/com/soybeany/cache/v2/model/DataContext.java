package com.soybeany.cache.v2.model;

import lombok.RequiredArgsConstructor;

/**
 * 数据上下文
 *
 * @author Soybeany
 * @date 2020/11/19
 */
@RequiredArgsConstructor
public class DataContext<Param> {

    public final String dataDesc;
    public final String storageId;
    public final String paramDesc;
    public final String paramKey;
    public final Param param;

}
