package com.soybeany.cache.v2.model;

import com.soybeany.cache.v2.contract.ILogger;

import java.util.Objects;

/**
 * 数据上下文
 *
 * @author Soybeany
 * @date 2020/11/19
 */
public class DataContext<P> {

    public final Core core;
    public final Param<P> param;

    public DataContext(Core core, Param<P> param) {
        this.core = core;
        this.param = param;
    }

    public static class Core {
        public final String dataDesc;
        public final String storageId;
        public final ILogger logger;

        public Core(String dataDesc, String storageId, ILogger logger) {
            this.dataDesc = dataDesc;
            this.storageId = storageId;
            this.logger = logger;
        }
    }

    public static class Param<P> {
        public final String paramDesc;
        public final String paramKey;
        public final P param;

        public Param(String paramDesc, String paramKey, P param) {
            this.paramDesc = paramDesc;
            this.paramKey = paramKey;
            this.param = param;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Param<?> param = (Param<?>) o;
            return Objects.equals(paramKey, param.paramKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(paramKey);
        }
    }

}
