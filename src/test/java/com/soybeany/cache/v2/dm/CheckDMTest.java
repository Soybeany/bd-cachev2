package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.ICacheChecker;
import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.contract.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

public class CheckDMTest {

    private final long pTtl = 500;
    private int v = 0;

    private final ICacheChecker<String, Integer> checker = (param, dataPack) -> dataPack.getData() != v;

    private final DataManager<String, Integer> dataManager = DataManager.Builder
            .get("数据检查测试", s -> v++)
            .withCache(new LruMemCacheStorage.Builder<String, Integer>().pTtl(pTtl).build())
            .enableDataCheck(200, checker)
            .logger(new ConsoleLogger<>())
            .build();

    @Test
    public void checkTest() throws Exception {
        String key = "123";
        DataPack<Integer> pack;
        // 从数据源获取
        pack = dataManager.getDataPack(key);
        assert pack.provider instanceof IDatasource;

        Thread.sleep(100);
        // 检查周期内，缓存有效
        pack = dataManager.getDataPack(key);
        assert pack.provider instanceof ICacheStorage;

        Thread.sleep(300);
        // 缓存周期内，检查周期外，重新从数据源获取
        pack = dataManager.getDataPack(key);
        assert pack.provider instanceof IDatasource;
        assert pack.pTtl == pTtl;

        Thread.sleep(500);
        // 缓存周期外，重新从数据源获取
        pack = dataManager.getDataPack(key);
        assert pack.provider instanceof IDatasource;
        // 数据源访问次数
        assert v == 3;
    }

    @Test
    public void checkNowTest() throws Exception {
        String key = "456";
        DataPack<Integer> pack;

        pack = dataManager.getDataPack(key);
        assert pack.provider instanceof IDatasource;

        Thread.sleep(100);

        pack = dataManager.getDataPack(key);
        assert pack.provider instanceof ICacheStorage;

        boolean needUpdate = dataManager.checkCache(key, checker);
        assert needUpdate;

        pack = dataManager.getDataPack(key);
        assert pack.provider instanceof IDatasource;

        assert v == 2;
    }

}
