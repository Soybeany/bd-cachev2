package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
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
            .logger(new ConsoleLogger())
            .build();

    @Test
    public void test_异常缓存和读取() {
        DataPack<String> data;
        // 从数据源抛出异常
        data = dataManager.getDataPack(null);
        assert (!data.norm() && datasource == data.provider) : "数据源异常应返回异常包";
        // 抛出的是缓存了的异常
        data = dataManager.getDataPack(null);
        assert (!data.norm() && cacheStorage == data.provider) : "异常应被缓存，从缓存读取";
    }

    @Test
    public void test_异常缓存过期后重新访问数据源() throws Exception {
        ICacheStorage<String, String> shortTtlStorage = new LruMemCacheStorage.Builder<String, String>().pTtl(100).build();
        DataManager<String, String> manager = DataManager.Builder
                .get("异常过期测试", datasource)
                .withCache(shortTtlStorage)
                .logger(new ConsoleLogger())
                .build();
        // 第一次从数据源抛出异常，缓存异常
        DataPack<String> pack = manager.getDataPack(null);
        assert !pack.norm() : "首次应返回异常包";
        assert datasource == pack.provider : "应来自数据源";
        // 第二次从缓存读取异常
        pack = manager.getDataPack(null);
        assert !pack.norm() : "第二次应返回缓存的异常包";
        assert shortTtlStorage == pack.provider : "应来自缓存";
        // 等待缓存过期
        Thread.sleep(150);
        // 缓存过期后，再次从数据源获取（重新抛异常）
        pack = manager.getDataPack(null);
        assert !pack.norm() : "缓存过期后再次返回异常包";
        assert datasource == pack.provider : "应重新来自数据源";
    }

    @Test
    public void test_异常缓存被移除后重新访问数据源() throws Exception {
        DataManager<String, String> manager = DataManager.Builder
                .get("异常移除测试", datasource)
                .withCache(cacheStorage)
                .logger(new ConsoleLogger())
                .build();
        // 缓存异常
        manager.getDataPack(null);
        // 移除缓存
        manager.removeCache(null);
        // 应再次从数据源获取（重新抛异常）
        DataPack<String> pack = manager.getDataPack(null);
        assert !pack.norm() : "移除缓存后应重新抛出异常";
        assert datasource == pack.provider : "应来自数据源";
    }
}
