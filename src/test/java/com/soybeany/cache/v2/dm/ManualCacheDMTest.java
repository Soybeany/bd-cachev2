package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * 测试手动缓存/批量缓存功能<br>
 * 对应{@link com.soybeany.cache.v2.core.DataManager#cacheData}和{@link com.soybeany.cache.v2.core.DataManager#batchCacheData}
 */
public class ManualCacheDMTest {

    private final IDatasource<String, String> datasource = s -> {
        throw new RuntimeException("不应访问数据源");
    };

    private final ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(60_000).build();

    private final DataManager<String, String> dataManager = DataManager.Builder
            .get("手动缓存测试", datasource)
            .withCache(storage)
            .logger(new ConsoleLogger())
            .build();

    // ********************cacheData********************

    @Test
    public void cacheData_手动缓存数据可正常读取() {
        String key = "manual_key";
        String value = "manual_value";
        // 手动缓存数据
        dataManager.cacheData(key, value);
        // 通过getDataPack读取 (不应访问数据源)
        DataPack<String> pack = dataManager.getDataPack(key);
        assert pack.norm();
        assert storage.equals(pack.provider);
        assert value.equals(pack.getData());
    }

    @Test
    public void cacheData_覆盖已有缓存数据() {
        String key = "override_key";
        // 第一次手动缓存
        dataManager.cacheData(key, "old_value");
        assert "old_value".equals(dataManager.getData(key));
        // 第二次手动缓存覆盖
        dataManager.cacheData(key, "new_value");
        assert "new_value".equals(dataManager.getData(key));
    }

    // ********************cacheException********************

    @Test
    public void cacheException_手动缓存异常可读取到异常() {
        String key = "err_key";
        RuntimeException ex = new RuntimeException("手动缓存的异常");
        dataManager.cacheException(key, ex);
        DataPack<String> pack = dataManager.getDataPack(key);
        assert !pack.norm();
        assert storage.equals(pack.provider);
        assert "手动缓存的异常".equals(pack.dataCore.exception.getMessage());
    }

    @Test(expected = RuntimeException.class)
    public void cacheException_手动缓存异常通过getData抛出() {
        String key = "err_key2";
        dataManager.cacheException(key, new RuntimeException("getData抛出手动缓存异常"));
        dataManager.getData(key);
    }

    // ********************batchCacheData********************

    @Test
    public void batchCacheData_批量缓存数据可正常读取() {
        Map<String, String> dataMap = new HashMap<>();
        dataMap.put("batch_1", "value1");
        dataMap.put("batch_2", "value2");
        dataMap.put("batch_3", "value3");
        dataManager.batchCacheData(dataMap);
        // 验证所有数据都能从缓存读取
        assert "value1".equals(dataManager.getData("batch_1"));
        assert "value2".equals(dataManager.getData("batch_2"));
        assert "value3".equals(dataManager.getData("batch_3"));
        // 验证提供者是storage
        assert storage.equals(dataManager.getDataPack("batch_1").provider);
    }

    // ********************batchCache********************

    @Test
    public void batchCache_混合缓存数据和异常() {
        Map<String, String> dataMap = new HashMap<>();
        dataMap.put("mixed_1", "data_val");
        Map<String, RuntimeException> exMap = new HashMap<>();
        exMap.put("mixed_2", new RuntimeException("batch异常"));
        dataManager.batchCache(dataMap, exMap);
        // 验证正常数据
        DataPack<String> pack1 = dataManager.getDataPack("mixed_1");
        assert pack1.norm();
        assert "data_val".equals(pack1.getData());
        // 验证异常数据
        DataPack<String> pack2 = dataManager.getDataPack("mixed_2");
        assert !pack2.norm();
        assert "batch异常".equals(pack2.dataCore.exception.getMessage());
    }

    @Test
    public void batchCache_仅缓存异常() {
        Map<String, RuntimeException> exMap = new HashMap<>();
        exMap.put("ex_only_1", new RuntimeException("异常1"));
        exMap.put("ex_only_2", new RuntimeException("异常2"));
        dataManager.batchCache(null, exMap);
        // 验证异常数据
        assert !dataManager.getDataPack("ex_only_1").norm();
        assert !dataManager.getDataPack("ex_only_2").norm();
    }

    @Test
    public void batchCache_仅缓存数据() {
        Map<String, String> dataMap = new HashMap<>();
        dataMap.put("data_only_1", "val1");
        dataManager.batchCache(dataMap, null);
        assert "val1".equals(dataManager.getData("data_only_1"));
    }
}
