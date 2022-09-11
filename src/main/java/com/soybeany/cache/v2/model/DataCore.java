package com.soybeany.cache.v2.model;

import com.google.gson.Gson;

import java.lang.reflect.Type;

/**
 * 数据核心，包含数据/异常
 * <br>Created by Soybeany on 2020/11/24.
 */
public class DataCore<Data> {

    private static final Gson GSON = new Gson();

    public final boolean norm; // 是否为正常数据
    public final Data data; // 数据
    public final RuntimeException exception; // 相关的异常

    public static <Data> String toJson(DataCore<Data> dataCore) {
        JsonInfo jsonInfo = new JsonInfo();
        jsonInfo.norm = dataCore.norm;
        if (dataCore.norm) {
            jsonInfo.dataJson = GSON.toJson(dataCore.data);
        } else {
            ExceptionInfo info = new ExceptionInfo();
            info.clazz = dataCore.exception.getClass().getName();
            info.json = GSON.toJson(dataCore.exception);
            jsonInfo.exceptionJson = info;
        }
        return GSON.toJson(jsonInfo);
    }

    /**
     * @param dataType {@link #data}数据的类型
     */
    public static <Data> DataCore<Data> fromJson(String json, Type dataType) throws ClassNotFoundException {
        JsonInfo jsonInfo = GSON.fromJson(json, JsonInfo.class);
        if (jsonInfo.norm) {
            return DataCore.fromData(GSON.fromJson(jsonInfo.dataJson, dataType));
        } else {
            ExceptionInfo exceptionInfo = jsonInfo.exceptionJson;
            return DataCore.fromException((RuntimeException) GSON.fromJson(exceptionInfo.json, Class.forName(exceptionInfo.clazz)));
        }
    }

    public static <Data> DataCore<Data> fromData(Data data) {
        return new DataCore<>(true, data, null);
    }

    public static <Data> DataCore<Data> fromException(RuntimeException e) {
        return new DataCore<>(false, null, e);
    }

    private DataCore(boolean norm, Data data, RuntimeException exception) {
        this.norm = norm;
        this.data = data;
        this.exception = exception;
    }

    private static class JsonInfo {
        public boolean norm; // 是否为正常数据
        public String dataJson; // 数据
        public ExceptionInfo exceptionJson; // 相关的异常
    }

    private static class ExceptionInfo {
        public String clazz;
        public String json;
    }
}
