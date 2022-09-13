package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import lombok.AllArgsConstructor;
import org.junit.Test;

/**
 * @author Soybeany
 * @date 2022/9/13
 */
public class DeepCopyDMTest {

    private final IDatasource<String, Data> datasource = key -> new Data(key, 123);

    private final DataManager<String, Data> dataManager1 = DataManager.Builder
            .get("无拷贝测试", datasource)
            .withCache(new LruMemCacheStorage.Builder<String, Data>().build())
            .logger(new ConsoleLogger<>())
            .build();

    private final DataManager<String, Data> dataManager2 = DataManager.Builder
            .get("深拷贝测试", datasource)
            .withCache(new LruMemCacheStorage.Builder<String, Data>().copyData(Data.class).build())
            .logger(new ConsoleLogger<>())
            .build();

    @Test
    public void test() {
        String key = "成功";
        // 无拷贝为同一副本
        Data data1 = dataManager1.getData(key);
        Data data2 = dataManager1.getData(key);
        assert data1 == data2;
        assert key.equals(data2.key);
        // 深拷贝为不同副本
        Data data3 = dataManager2.getData(key);
        Data data4 = dataManager2.getData(key);
        assert data3 != data4;
        assert key.equals(data4.key);
    }

    @AllArgsConstructor
    private static class Data {
        String key;
        Integer b;
    }

}
