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
    Data onGetData(Param param);

    /**
     * 为指定的数据设置超时
     *
     * @return 指定数据的超时，单位为millis
     */
    default long onSetupExpiry(Param param, Data data) {
        return onSetupExpiry(data);
    }

    /**
     * 为指定的数据设置超时
     *
     * @return 指定数据的超时，单位为millis
     */
    default long onSetupExpiry(Data data) {
        return Long.MAX_VALUE;
    }

    /**
     * 为指定的异常设置超时
     *
     * @return 指定异常的超时，单位为millis
     */
    default long onSetupExpiry(Param param, Exception e) {
        return onSetupExpiry(e);
    }

    /**
     * 为指定的异常设置超时
     *
     * @return 指定异常的超时，单位为millis
     */
    default long onSetupExpiry(Exception e) {
        return Long.MAX_VALUE;
    }
}
