package com.soybeany.cache.v2.contract;

/**
 * 数据源
 *
 * @author Soybeany
 * @date 2020/1/19
 */
public interface IDatasource<Param, Data> {
    /**
     * 从数据源获取数据
     *
     * @param param 请求参数
     * @return 数据
     */
    Data onGetData(Param param) throws Exception;

    /**
     * 为指定的数据设置超时
     *
     * @return 指定数据的超时，单位为millis
     */
    default int onSetupExpiry(Data data) throws Exception {
        return Integer.MAX_VALUE;
    }

    /**
     * 为指定的异常设置超时
     *
     * @return 指定异常的超时，单位为millis
     */
    default int onSetupExpiry(Exception e) {
        return Integer.MAX_VALUE;
    }
}
