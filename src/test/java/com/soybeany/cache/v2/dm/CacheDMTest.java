package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

/**
 * 测试getCache/getCacheDataPack API（仅取缓存、不访问数据源）
 */
public class CacheDMTest {

    private final IDatasource<String, String> datasource = s -> "ok";

    private final ICacheStorage<String, String> shortTtlStorage = new LruMemCacheStorage.Builder<String, String>()
            .pTtl(100)
            .build();

    private final DataManager<String, String> dataManager = DataManager.Builder
            .get("缓存读取测试", datasource)
            .withCache(shortTtlStorage)
            .logger(new ConsoleLogger())
            .build();

    // ********************getCacheDataPack********************

    @Test
    public void getCacheDataPack_无缓存时返回异常包() {
        DataPack<String> pack = dataManager.getCacheDataPack("no_cache");
        assert !pack.norm();
        assert pack.dataCore.exception instanceof NoCacheException;
    }

    @Test
    public void getCacheDataPack_有缓存时返回正常包() {
        String key = "key_norm";
        dataManager.getDataPack(key);
        DataPack<String> pack = dataManager.getCacheDataPack(key);
        assert pack.norm();
        assert shortTtlStorage.equals(pack.provider);
        assert "ok".equals(pack.getData());
    }

    @Test
    public void getCacheDataPack_缓存过期仍返回数据() throws Exception {
        String key = "key_expired";
        dataManager.getDataPack(key);
        Thread.sleep(150);
        DataPack<String> pack = dataManager.getCacheDataPack(key);
        assert pack.norm();
        assert shortTtlStorage.equals(pack.provider);
        assert "ok".equals(pack.getData());
    }

    @Test
    public void getCacheDataPack_缓存的是异常() {
        IDatasource<String, String> errDs = s -> {
            throw new RuntimeException("模拟异常");
        };
        DataManager<String, String> errManager = DataManager.Builder
                .get("异常缓存测试", errDs)
                .withCache(new LruMemCacheStorage.Builder<String, String>().build())
                .build();
        String key = "key_err";
        errManager.getDataPack(key);
        DataPack<String> pack = errManager.getCacheDataPack(key);
        assert !pack.norm();
    }

    // ********************getCache********************

    @Test
    public void getCache_无缓存时抛异常() {
        try {
            dataManager.getCache("no_cache");
            throw new RuntimeException("不应执行到此");
        } catch (NoCacheException e) {
            // 预期
        }
    }

    @Test
    public void getCache_有缓存返回数据() {
        String key = "key_norm_get";
        dataManager.getDataPack(key);
        String data = dataManager.getCache(key);
        assert "ok".equals(data);
    }

    @Test
    public void getCache_缓存过期仍返回数据() throws Exception {
        String key = "key_expired_get";
        dataManager.getDataPack(key);
        Thread.sleep(150);
        String data = dataManager.getCache(key);
        assert "ok".equals(data);
    }

    @Test
    public void getCache_缓存的是异常时抛异常() {
        IDatasource<String, String> errDs = s -> {
            throw new RuntimeException("模拟异常");
        };
        DataManager<String, String> errManager = DataManager.Builder
                .get("异常缓存测试", errDs)
                .withCache(new LruMemCacheStorage.Builder<String, String>().build())
                .build();
        String key = "key_err_get";
        errManager.getDataPack(key);
        try {
            errManager.getCache(key);
            throw new RuntimeException("不应执行到此");
        } catch (RuntimeException e) {
            assert "模拟异常".equals(e.getMessage());
        }
    }

}
