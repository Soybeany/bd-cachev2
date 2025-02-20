package com.soybeany.cache.v2.dm;

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

    private final DataManager<String, Integer> dataManager = DataManager.Builder
            .get("数据检查测试", s -> v++)
            .withCache(new LruMemCacheStorage.Builder<String, Integer>().pTtl(pTtl).build())
            .enableDataCheck(200, (param, dataPack) -> dataPack.getData() != v)
            .logger(new ConsoleLogger<>())
            .build();

    @Test
    public void checkTest() throws Exception {
        String key = "123";
        DataPack<Integer> pack;

        pack = dataManager.getDataPack(key);
        assert pack.provider instanceof IDatasource;

        Thread.sleep(100);

        pack = dataManager.getDataPack(key);
        assert pack.provider instanceof ICacheStorage;

        Thread.sleep(300);

        pack = dataManager.getDataPack(key);
        assert pack.provider instanceof IDatasource;
        assert pack.pTtl == pTtl;

        Thread.sleep(500);

        pack = dataManager.getDataPack(key);
        assert pack.provider instanceof IDatasource;
    }

}
