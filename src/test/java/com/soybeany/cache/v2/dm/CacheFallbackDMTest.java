package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataCore;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

/**
 * 测试短超时回退模式<br>
 * 对应{@link com.soybeany.cache.v2.core.StorageManager#getDataPackWithCacheFallback}
 */
public class CacheFallbackDMTest {

    private final IDatasource<String, String> slowDatasource = s -> {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException ignore) {
        }
        return "新数据";
    };

    private final IDatasource<String, String> fastDatasource = s -> {
        System.out.println("快速数据源被访问: " + s);
        return "新数据";
    };

    // ********************getDataPackWithCacheFallback********************

    @Test
    public void fallback_数据源快速响应时返回新数据() {
        DataManager<String, String> manager = createManager(fastDatasource);
        // fallback调用应直接返回新数据
        DataPack<String> pack = manager.getDataPackWithCacheFallback("key");
        assert pack.norm();
        assert "新数据".equals(pack.getData());
    }

    @Test
    public void fallback_数据源慢时回退到过期缓存() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(200).build();
        // 必须启用续期，否则异步线程的onGetCache会移除过期缓存，导致fallback找不到
        DataManager<String, String> manager = createManagerWithRenew(storage, slowDatasource);
        String key = "fallback_key";
        // 用快速数据源写入缓存
        manager.getDataPack(key, fastDatasource);
        // 等待缓存过期
        Thread.sleep(250);
        // fallback调用，短超时应从过期缓存返回
        DataPack<String> pack = manager.getDataPackWithCacheFallback(key);
        // 应快速返回（远小于3秒的数据源耗时）
        assert pack.norm() : "fallback应返回正常数据包";
        assert "新数据".equals(pack.getData());
    }

    @Test
    public void fallback_无过期缓存时回退到异常包() {
        DataManager<String, String> manager = createManager(slowDatasource);
        // 从未缓存过任何数据
        DataPack<String> pack = manager.getDataPackWithCacheFallback("no_cache_key");
        assert !pack.norm() : "无缓存时应返回异常包";
    }

    @Test
    public void fallback_自定义超时时间() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(200).build();
        DataManager<String, String> manager = createManagerWithRenew(storage, slowDatasource);
        String key = "custom_timeout";
        // 用快速数据源写入缓存
        manager.getDataPack(key, fastDatasource);
        Thread.sleep(250);
        // 使用极短超时（100ms），应回退到过期缓存
        DataPack<String> pack = manager.getDataPackWithCacheFallback(key, 100L);
        assert pack.norm() : "应回退到过期缓存";
        assert "新数据".equals(pack.getData());
    }

    @Test
    public void fallback_默认超时时间() {
        DataManager<String, String> manager = createManager(fastDatasource);
        // 使用默认超时（DEFAULT_QUICK_TIMEOUT_MS = 2000ms）
        DataPack<String> pack = manager.getDataPackWithCacheFallback("default_key");
        assert pack.norm();
        assert "新数据".equals(pack.getData());
    }

    @Test
    public void fallback_指定数据源() {
        DataManager<String, String> manager = createManager(slowDatasource);
        // 使用快速数据源作为参数传入
        DataPack<String> pack = manager.getDataPackWithCacheFallback("spec_ds", fastDatasource);
        assert pack.norm();
        assert "新数据".equals(pack.getData());
    }

    @Test
    public void fallback_指定数据源和超时时间() {
        DataManager<String, String> manager = createManager(slowDatasource);
        // 使用快速数据源和自定义超时
        DataPack<String> pack = manager.getDataPackWithCacheFallback("spec_ds_timeout", fastDatasource, 500L);
        assert pack.norm();
        assert "新数据".equals(pack.getData());
    }

    @Test
    public void fallback_自定义回退处理逻辑() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(200).build();
        DataManager<String, String> manager = createManagerWithRenew(storage, slowDatasource);
        String key = "custom_fallback";
        // 用快速数据源写入缓存
        manager.getDataPack(key, fastDatasource);
        Thread.sleep(250);
        // 使用自定义fallback处理器
        DataPack<String> pack = manager.getDataPackWithCacheFallback(
                key, null, 500L,
                expiredPack -> {
                    if (expiredPack.norm()) {
                        return expiredPack;
                    }
                    return new DataPack<>(DataCore.fromData("fallback默认值"), expiredPack.provider, 0);
                }
        );
        assert pack.norm();
        assert "新数据".equals(pack.getData());
    }

    // ********************getDataPackWithCacheFallback(无参数数据源)********************

    @Test
    public void fallback_无参数数据源重载() {
        DataManager<String, String> manager = createManager(fastDatasource);
        DataPack<String> pack = manager.getDataPackWithCacheFallback("key");
        assert pack.norm();
    }

    @Test
    public void fallback_双参数重载() {
        DataManager<String, String> manager = createManager(fastDatasource);
        DataPack<String> pack = manager.getDataPackWithCacheFallback("key", 100L);
        assert pack.norm();
    }

    // ********************getDataWithCacheFallback********************

    @Test
    public void getDataWithCacheFallback_正常返回数据() {
        DataManager<String, String> manager = createManager(fastDatasource);
        String data = manager.getDataWithCacheFallback("key");
        assert "新数据".equals(data);
    }

    @Test
    public void getDataWithCacheFallback_指定超时() {
        DataManager<String, String> manager = createManager(fastDatasource);
        String data = manager.getDataWithCacheFallback("key", 100L);
        assert "新数据".equals(data);
    }

    // ********************内部方法********************

    private DataManager<String, String> createManager(IDatasource<String, String> ds) {
        return createManager(new LruMemCacheStorage.Builder<String, String>().pTtl(200).build(), ds);
    }

    private DataManager<String, String> createManager(ICacheStorage<String, String> storage, IDatasource<String, String> ds) {
        return newBuilder(storage, ds).build();
    }

    private DataManager<String, String> createManagerWithRenew(ICacheStorage<String, String> storage, IDatasource<String, String> ds) {
        return newBuilder(storage, ds).enableRenewExpiredCache(true).build();
    }

    private DataManager.Builder<String, String> newBuilder(ICacheStorage<String, String> storage, IDatasource<String, String> ds) {
        return DataManager.Builder
                .get("回退测试", ds)
                .withCache(storage)
                .logger(new ConsoleLogger());
    }
}
