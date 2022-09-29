package com.soybeany.cache.v2.model;

import com.soybeany.cache.v2.contract.ILogger;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

/**
 * 数据上下文
 *
 * @author Soybeany
 * @date 2020/11/19
 */
@RequiredArgsConstructor
public class DataContext<P> {

    public final Core<P, ?> core;
    public final Param<P> param;

    @RequiredArgsConstructor
    public static class Core<P, Data> {
        public final String dataDesc;
        public final String storageId;
        public final ILogger<P, Data> logger;

    }

    @EqualsAndHashCode
    @RequiredArgsConstructor
    public static class Param<P> {
        public final String paramDesc;
        public final String paramKey;
        public final P param;
    }

}
