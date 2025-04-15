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
public class MultiExpiryDMTest {

    private final IDatasource<String, String> datasource = s -> {
        System.out.println("“" + s + "”access datasource");
        return UUID.randomUUID().toString();
    };

    private final ICacheStorage<String, String> lruStorage = new LruMemCacheStorage.Builder<String, String>().pTtl(800).build();
    private final ICacheStorage<String, String> dbStorage = new DBSimulationStorage<>(1000);

    private final DataManager<String, String> dataManager = DataManager.Builder
            .get("MultiExpiry", datasource)
            .withCache(lruStorage)
            .withCache(dbStorage)
            .logger(new ConsoleLogger())
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
        // 休眠一个比lru时间长，但比db时间短的时间
        Thread.sleep(900);
        // lru已失效，但db未失效
        pack = dataManager.getDataPack(key);
        assert dbStorage.equals(pack.provider);
        // lru仍生效
        pack = dataManager.getDataPack(key);
        assert lruStorage.equals(pack.provider);
        // 休眠一个短时间，使db也失效
        Thread.sleep(200);
        // 全部缓存已失效，再次访问数据源
        pack = dataManager.getDataPack(key);
        assert datasource.equals(pack.provider);
    }

}
