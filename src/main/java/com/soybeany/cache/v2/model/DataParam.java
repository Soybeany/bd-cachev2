package com.soybeany.cache.v2.model;

import java.util.Objects;

public class DataParam<T> {
    public final String paramDesc;
    public final String paramKey;
    public final T value;

    public DataParam(String paramDesc, String paramKey, T value) {
        this.paramDesc = paramDesc;
        this.paramKey = paramKey;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataParam<?> param = (DataParam<?>) o;
        return Objects.equals(paramKey, param.paramKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(paramKey);
    }
}
