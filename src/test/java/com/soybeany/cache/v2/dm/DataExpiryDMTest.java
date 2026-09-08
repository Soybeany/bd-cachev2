package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.exception.BdCacheException;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试各级缓存的有效期函数配置({@link com.soybeany.cache.v2.storage.StdStorageBuilder#ttl})
 * <br>某级缓存实际使用的有效期为min(该级ttl函数返回值，数据/异常剩余有效期)，同时适用于数据源与手动缓存场景
 */
public class DataExpiryDMTest {

    private final AtomicInteger accessCount = new AtomicInteger();

    private final IDatasource<String, String> datasource = s -> {
        accessCount.incrementAndGet();
        return "data_" + s;
    };

    // ********************ttl函数********************

    @Test
    public void ttl函数设置的有效期生效() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .ttl((p, core) -> 500L).build();
        DataManager<String, String> dataManager = DataManager.Builder
                .get("ttl函数有效期", datasource)
                .withCache(storage)
                .build();
        // 首次访问数据源并缓存
        dataManager.getData("key");
        assert 1 == accessCount.get();
        // 休眠超过ttl函数返回的有效期，缓存应失效并再次访问数据源
        Thread.sleep(600);
        dataManager.getData("key");
        assert 2 == accessCount.get();
    }

    @Test
    public void ttl函数可区分正常数据与异常的有效期() throws Exception {
        AtomicInteger failCount = new AtomicInteger();
        IDatasource<String, String> failOnceDatasource = s -> {
            accessCount.incrementAndGet();
            // 首次访问抛出异常，之后返回正常数据
            if (failCount.getAndIncrement() < 1) {
                throw new RuntimeException("首次访问异常");
            }
            return "data_" + s;
        };
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .ttl((p, core) -> core.norm ? 60_000L : 400L).build();
        DataManager<String, String> dataManager = DataManager.Builder
                .get("ttl函数norm分支", failOnceDatasource)
                .withCache(storage)
                .build();
        // 首次访问，缓存异常
        assert !dataManager.getDataPack("key").norm();
        assert 1 == accessCount.get();
        // 短时间内再次访问，命中异常缓存
        assert !dataManager.getDataPack("key").norm();
        assert 1 == accessCount.get();
        // 休眠超过异常有效期，缓存失效并访问数据源获得正常数据
        Thread.sleep(500);
        assert dataManager.getDataPack("key").norm();
        assert 2 == accessCount.get();
    }

    @Test
    public void 手动缓存遵循ttl函数的有效期() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .ttl((p, core) -> 400L).build();
        DataManager<String, String> dataManager = DataManager.Builder
                .get("手动缓存ttl函数", datasource)
                .withCache(storage)
                .build();
        // 手动缓存数据，此时不访问数据源
        dataManager.cacheData("key", "手动数据");
        assert 0 == accessCount.get();
        assert "手动数据".equals(dataManager.getData("key"));
        // 休眠超过有效期，缓存应失效并访问数据源
        Thread.sleep(500);
        assert "data_key".equals(dataManager.getData("key"));
        assert 1 == accessCount.get();
    }

    @Test
    public void 数据提升时使用min_剩余有效期与ttl函数返回值() throws Exception {
        ICacheStorage<String, String> firstStorage = new LruMemCacheStorage.Builder<String, String>()
                .pTtl(60_000).build();
        ICacheStorage<String, String> secondStorage = new LruMemCacheStorage.Builder<String, String>()
                .pTtl(1000).build();
        DataManager<String, String> dataManager = DataManager.Builder
                .get("提升min验证", datasource)
                .withCache(firstStorage)
                .withCache(secondStorage)
                .build();
        // 首次访问，一级缓存60s，二级缓存1s
        dataManager.getData("key");
        assert 1 == accessCount.get();
        // 仅失效一级缓存，保留二级缓存
        dataManager.invalidCache("key", 0);
        Thread.sleep(300);
        // 一级未命中，二级命中，提升至一级缓存，有效期为min(二级剩余约700ms，一级60s)≈700ms
        dataManager.getData("key");
        assert 1 == accessCount.get();
        // 休眠至超过提升数据的预期失效时间(t0+1000)，应再次访问数据源
        Thread.sleep(800);
        dataManager.getData("key");
        assert 2 == accessCount.get();
    }

    @Test
    public void ttl函数返回无效值时缓存结果中包含异常() {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .ttl((p, core) -> null).build();
        DataManager<String, String> dataManager = DataManager.Builder
                .get("ttl函数无效值", datasource)
                .withCache(storage)
                .build();
        // 手动缓存时，ttl函数的异常会体现在返回结果中
        Map<Integer, Exception> result = dataManager.cacheData("key", "value");
        assert result.get(0) instanceof BdCacheException;
    }

}
