package com.soybeany.cache.v2.contract.user;

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
}
