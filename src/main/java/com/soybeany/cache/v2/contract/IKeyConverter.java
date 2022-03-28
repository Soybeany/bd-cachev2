package com.soybeany.cache.v2.contract;

/**
 * key转换器
 *
 * @author Soybeany
 * @date 2020/1/19
 */
public interface IKeyConverter<Param> {

    /**
     * 从param中获得key
     *
     * @param param 入参
     * @return 键
     */
    String getKey(Param param);

    class Std implements IKeyConverter<String> {
        @Override
        public String getKey(String s) {
            return s;
        }
    }
}
