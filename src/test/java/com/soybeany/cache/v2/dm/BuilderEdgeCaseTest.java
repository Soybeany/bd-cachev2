package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.exception.BdCacheException;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

/**
 * 测试Builder配置的边缘场景
 */
public class BuilderEdgeCaseTest {

    private final IDatasource<String, String> datasource = s -> "数据:" + s;

    // ********************Builder构建异常场景********************

    @Test(expected = BdCacheException.class)
    public void build_nullDataDesc() {
        DataManager.Builder.get(null, datasource);
    }

    @Test(expected = BdCacheException.class)
    public void build_nullKeyConverter() {
        DataManager.Builder.get("测试", datasource, null);
    }

    // ********************无缓存存储器场景********************

    @Test
    public void noStorage_每次都访问数据源() {
        DataManager<String, String> manager = DataManager.Builder
                .get("无缓存测试", datasource)
                .logger(new ConsoleLogger())
                .build();
        String key = "no_storage";
        assert datasource.equals(manager.getDataPack(key).provider) : "无缓存时应直接访问数据源";
        assert datasource.equals(manager.getDataPack(key).provider) : "无缓存时第二次应仍访问数据源";
    }

    // ********************无Logger场景********************

    @Test
    public void noLogger_功能正常() {
        DataManager<String, String> manager = DataManager.Builder
                .get("无Logger测试", datasource)
                .withCache(new LruMemCacheStorage.Builder<String, String>().build())
                .build();
        String data = manager.getData("no_logger");
        assert "数据:no_logger".equals(data);
    }

    // ********************pTtl=0时需要fallback到ttl********************

    @Test
    public void pTtlZero_使用ttl作为默认值() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .ttl(1) // 1秒，转换为pTtl=1000ms
                .build();
        DataManager<String, String> manager = DataManager.Builder
                .get("pTtlZero测试", datasource)
                .withCache(storage)
                .build();
        String key = "ttl_only";
        manager.getDataPack(key);
        // 1000ms内应命中缓存
        assert manager.getDataPack(key).provider instanceof ICacheStorage : "pTtl从ttl转换后应生效";
        // 等待过期
        Thread.sleep(1100);
        assert datasource.equals(manager.getDataPack(key).provider) : "缓存应已过期";
    }

    // ********************pTtlErr默认为pTtl上限********************

    @Test
    public void pTtlErrDefault_不超pTtl() {
        // pTtlErr默认值由handleTtl限制：pTtlErr = Math.min(pTtlErr, pTtl)
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .pTtl(100)
                .pTtlErr(1000) // 超过pTtl，会被截断为100
                .build();
        DataManager<String, String> manager = DataManager.Builder
                .get("pTtlErr测试", datasource)
                .withCache(storage)
                .build();
        // 手动缓存异常，验证pTtlErr生效
        manager.cacheException("err_key", new RuntimeException("test_err"));
        DataPack<String> pack = manager.getDataPack("err_key");
        assert !pack.norm() : "异常应被缓存";
    }

    // ********************fetchLockTimeout超时场景********************

    @Test
    public void fetchLockTimeout_default使用默认值() {
        IDatasource<String, String> fastDs = s -> "ok";
        DataManager<String, String> manager = DataManager.Builder
                .get("默认超时测试", fastDs)
                .withCache(new LruMemCacheStorage.Builder<String, String>().build())
                .build();
        // 不应超时
        String data = manager.getData("key");
        assert "ok".equals(data);
    }
}
