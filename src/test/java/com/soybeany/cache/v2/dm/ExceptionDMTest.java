package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.contract.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

/**
 * <br>Created by Soybeany on 2020/10/16.
 */
public class ExceptionDMTest {

    IDatasource<String, String> datasource = s -> {
        throw new RuntimeException("测试");
    };

    ICacheStorage<String, String> cacheStorage = new LruMemCacheStorage.Builder<String, String>().build();

    private final DataManager<String, String> dataManager = DataManager.Builder
            .get("异常测试", datasource)
            .withCache(cacheStorage)
            .logger(new ConsoleLogger<>())
            .build();

    @Test
    public void test() {
        DataPack<String> data;
        // 从数据源抛出异常
        data = dataManager.getDataPack(null);
        assert (!data.norm() && datasource == data.provider);
        // 抛出的是缓存了的异常
        data = dataManager.getDataPack(null);
        assert (!data.norm() && cacheStorage == data.provider);
    }

}
