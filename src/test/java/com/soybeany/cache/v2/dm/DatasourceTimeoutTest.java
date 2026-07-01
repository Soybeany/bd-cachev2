package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.exception.CacheWaitException;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

public class DatasourceTimeoutTest {

    private static final long TIMEOUT_MS = 100;

    private final IDatasource<String, String> slowDatasource = s -> {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ignore) {
        }
        return "ok";
    };

    private final IDatasource<String, String> fastDatasource = s -> "ok";

    @Test
    public void datasourceTimeoutShouldExpireInGetDataPack() {
        DataManager<String, String> manager = createManager(slowDatasource);
        DataPack<String> pack = manager.getDataPack("key");
        assert !pack.norm();
        assert pack.dataCore.exception instanceof CacheWaitException;
    }

    @Test
    public void datasourceTimeoutShouldExpireInGetData() {
        DataManager<String, String> manager = createManager(slowDatasource);
        try {
            manager.getData("key");
            throw new RuntimeException("不应成功获取数据");
        } catch (CacheWaitException e) {
            System.out.println("预期的超时异常:" + e.getMessage());
        }
    }

    @Test
    public void datasourceTimeoutShouldExpireInGetDataPackDirectly() {
        DataManager<String, String> manager = createManager(slowDatasource);
        DataPack<String> pack = manager.getDataPackDirectly("key");
        assert !pack.norm();
        assert pack.dataCore.exception instanceof CacheWaitException;
    }

    @Test
    public void normalDatasourceShouldNotTimeout() {
        DataManager<String, String> manager = createManager(fastDatasource);
        String data = manager.getData("key");
        assert "ok".equals(data);
    }

    private DataManager<String, String> createManager(IDatasource<String, String> datasource) {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .capacity(3)
                .pTtl(200)
                .build();
        return DataManager.Builder
                .get("数据源超时测试", datasource)
                .withCache(storage)
                .logger(new ConsoleLogger())
                .datasourceTimeout(TIMEOUT_MS)
                .build();
    }
}
