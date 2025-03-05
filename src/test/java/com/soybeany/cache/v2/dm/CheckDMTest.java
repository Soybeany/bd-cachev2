package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.ICacheChecker;
import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.contract.IDatasource;
import com.soybeany.cache.v2.contract.IKeyConverter;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

public class CheckDMTest {

    private final long pTtl = 500;
    private int v = 0;
    private int c = 0;

    private final ICacheChecker<Integer, Integer> checker = (param, dataPack) -> {
        c++;
        return dataPack.getData() < param;
    };

    private final DataManager<Integer, Integer> dataManager = DataManager.Builder
            .get("数据检查测试", s -> v++, (IKeyConverter<Integer>) s -> "")
            .withCache(new LruMemCacheStorage.Builder<Integer, Integer>().pTtl(pTtl).build())
            .enableDataCheck(200, checker)
            .logger(new ConsoleLogger<>())
            .build();

    @Test
    public void checkTest() throws Exception {
        int key = 2;
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

        Thread.sleep(510);
        // 缓存周期外，重新从数据源获取
        pack = dataManager.getDataPack(key);
        assert pack.provider instanceof IDatasource;

        Thread.sleep(210);
        dataManager.getDataPack(key);
        dataManager.getDataPack(key);
        Thread.sleep(210);
        dataManager.getDataPack(key);

        // 数据源访问次数
        assert c == 3;
        assert v == 3;
    }

    @Test
    public void checkNowTest() throws Exception {
        int key = 4;
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
