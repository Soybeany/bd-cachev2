package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.contract.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

public class InvalidDMTest {

    private final IDatasource<String, String> normDatasource = s -> "ok";

    private final IDatasource<String, String> errDatasource = s -> {
        throw new RuntimeException("err");
    };

    private final ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(100).build();

    private final DataManager<String, String> dataManager = DataManager.Builder
            .get("失效测试", normDatasource)
            .withCache(storage)
            .enableRenewExpiredCache(true)
            .logger(new ConsoleLogger<>())
            .build();

    @Test
    public void test() throws Exception {
        DataPack<String> data;
        String key = "key";
        // 不报错
        dataManager.invalidCache(key);
        // 从数据源获取
        data = dataManager.getDataPack(key);
        assert data.norm() && normDatasource == data.provider;
        // 从缓存获取
        data = dataManager.getDataPack(key);
        assert data.norm() && storage == data.provider;
        // 从缓存(续期)获取
        dataManager.invalidCache(key);
        data = dataManager.getDataPack(key, errDatasource);
        assert data.norm() && storage == data.provider;
        // 从数据源获取
        Thread.sleep(200);
        data = dataManager.getDataPack(key);
        assert data.norm() && normDatasource == data.provider;
    }

    @Test
    public void test2() throws Exception {
        DataPack<String> data;
        String key1 = "key1";
        String key2 = "key2";
        // 不报错
        dataManager.invalidAllCache();
        // 从数据源获取
        data = dataManager.getDataPack(key1);
        assert data.norm() && normDatasource == data.provider;
        data = dataManager.getDataPack(key2);
        assert data.norm() && normDatasource == data.provider;
        // 从缓存获取
        data = dataManager.getDataPack(key1);
        assert data.norm() && storage == data.provider;
        data = dataManager.getDataPack(key2);
        assert data.norm() && storage == data.provider;
        // 从缓存(续期)获取
        dataManager.invalidAllCache();
        data = dataManager.getDataPack(key1, errDatasource);
        assert data.norm() && storage == data.provider;
        data = dataManager.getDataPack(key2, errDatasource);
        assert data.norm() && storage == data.provider;
        // 从数据源获取
        Thread.sleep(200);
        data = dataManager.getDataPack(key1);
        assert data.norm() && normDatasource == data.provider;
        data = dataManager.getDataPack(key2);
        assert data.norm() && normDatasource == data.provider;
    }

}
