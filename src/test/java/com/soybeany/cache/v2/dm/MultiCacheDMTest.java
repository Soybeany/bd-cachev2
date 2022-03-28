package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.component.DBSimulationStorage;
import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.contract.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

import java.util.UUID;

/**
 * <br>Created by Soybeany on 2020/10/16.
 */
public class MultiCacheDMTest {

    private final IDatasource<String, String> datasource = s -> {
        System.out.println("“" + s + "”access datasource");
        return UUID.randomUUID().toString();
    };

    private final ICacheStorage<String, String> lruStorage = new LruMemCacheStorage.Builder<String, String>().pTtl(500).build();
    private final ICacheStorage<String, String> dbStorage = new DBSimulationStorage<>();

    private final DataManager<String, String> dataManager = DataManager.Builder
            .get("MultiCache", datasource)
            .withCache(lruStorage)
            .withCache(dbStorage)
            .logger(new ConsoleLogger<>())
            .build();

    @Test
    public void test() throws Exception {
        String key = "key";
        // 一开始没有数据，应该访问数据源
        DataPack<String> pack = dataManager.getDataPack(key);
        assert datasource.equals(pack.provider);
        // 已经缓存了数据，应该访问lru
        pack = dataManager.getDataPack(key);
        assert lruStorage.equals(pack.provider);
        // 休眠一个比lru时间长的时间
        Thread.sleep(600);
        // lru缓存已失效，访问db
        pack = dataManager.getDataPack(key);
        assert dbStorage.equals(pack.provider);
        // 缓存重新建立
        pack = dataManager.getDataPack(key);
        assert lruStorage.equals(pack.provider);
    }

    @Test
    public void test2() {
        String key = "key";
        // 一开始没有数据，应该访问数据源
        DataPack<String> pack = dataManager.getDataPack(key);
        assert datasource.equals(pack.provider);
        // 已经缓存了数据，应该访问lru
        pack = dataManager.getDataPack(key);
        assert lruStorage.equals(pack.provider);
        // 清除了lru缓存，访问db
        dataManager.clearCache(0);
        pack = dataManager.getDataPack(key);
        assert dbStorage.equals(pack.provider);
        // 清除全部缓存，访问数据源
        dataManager.clearCache();
        pack = dataManager.getDataPack(key);
        assert datasource.equals(pack.provider);
    }

}
