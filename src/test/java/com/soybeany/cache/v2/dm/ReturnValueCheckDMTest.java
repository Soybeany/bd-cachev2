package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.component.FaultInjectStorage;
import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * 测试返回 {@code Map<Integer, Exception>} 的方法的返回值准确性
 */
public class ReturnValueCheckDMTest {

    private final IDatasource<String, String> datasource = s -> "data:" + s;

    private static ICacheStorage<String, String> createStorage() {
        return new LruMemCacheStorage.Builder<String, String>().pTtl(60_000).build();
    }

    // ==================== cacheData ====================

    @Test
    public void cacheData_成功时返回空Map() {
        ICacheStorage<String, String> s1 = createStorage();
        ICacheStorage<String, String> s2 = createStorage();
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(s2)
                .logger(new ConsoleLogger())
                .build();
        Map<Integer, Exception> result = manager.cacheData("key", "value");
        assertTrue("cacheData成功时应返回空Map", result.isEmpty());
    }

    @Test
    public void cacheData_单存储器失败返回对应异常() {
        ICacheStorage<String, String> s1 = createStorage();
        ICacheStorage<String, String> s2 = createStorage();
        FaultInjectStorage<String, String> faultyStorage = new FaultInjectStorage<>(s2);
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(faultyStorage)
                .logger(new ConsoleLogger())
                .build();

        RuntimeException expectedEx = new RuntimeException("cacheData模拟写入失败");
        faultyStorage.exOnCacheData(expectedEx);

        Map<Integer, Exception> result = manager.cacheData("key", "value");
        assertEquals("cacheData应包含1个异常", 1, result.size());
        assertTrue("cacheData应包含索引1的异常", result.containsKey(1));
        assertSame("cacheData的异常应与预期一致", expectedEx, result.get(1));
    }

    @Test
    public void cacheData_多存储器同时失败返回所有异常() {
        FaultInjectStorage<String, String> faultyS1 = new FaultInjectStorage<>(createStorage());
        FaultInjectStorage<String, String> faultyS2 = new FaultInjectStorage<>(createStorage());
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(faultyS1)
                .withCache(faultyS2)
                .logger(new ConsoleLogger())
                .build();

        RuntimeException ex1 = new RuntimeException("s1写入失败");
        RuntimeException ex2 = new RuntimeException("s2写入失败");
        faultyS1.exOnCacheData(ex1);
        faultyS2.exOnCacheData(ex2);

        Map<Integer, Exception> result = manager.cacheData("key", "value");
        assertEquals("cacheData应包含2个异常", 2, result.size());
        assertSame("cacheData索引0异常应正确", ex1, result.get(0));
        assertSame("cacheData索引1异常应正确", ex2, result.get(1));
    }

    // ==================== cacheException ====================

    @Test
    public void cacheException_成功时返回空Map() {
        ICacheStorage<String, String> s1 = createStorage();
        ICacheStorage<String, String> s2 = createStorage();
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(s2)
                .logger(new ConsoleLogger())
                .build();
        Map<Integer, Exception> result = manager.cacheException("key", new RuntimeException("模拟异常"));
        assertTrue("cacheException成功时应返回空Map", result.isEmpty());
    }

    @Test
    public void cacheException_单存储器失败返回对应异常() {
        ICacheStorage<String, String> s1 = createStorage();
        ICacheStorage<String, String> s2 = createStorage();
        FaultInjectStorage<String, String> faultyStorage = new FaultInjectStorage<>(s2);
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(faultyStorage)
                .logger(new ConsoleLogger())
                .build();

        RuntimeException expectedEx = new RuntimeException("cacheException模拟写入失败");
        faultyStorage.exOnCacheData(expectedEx);

        Map<Integer, Exception> result = manager.cacheException("key", new RuntimeException("模拟异常"));
        assertEquals("cacheException应包含1个异常", 1, result.size());
        assertTrue("cacheException应包含索引1的异常", result.containsKey(1));
        assertSame("cacheException的异常应与预期一致", expectedEx, result.get(1));
    }

    // ==================== batchCacheData ====================

    @Test
    public void batchCacheData_成功时返回空Map() {
        ICacheStorage<String, String> s1 = createStorage();
        ICacheStorage<String, String> s2 = createStorage();
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(s2)
                .logger(new ConsoleLogger())
                .build();
        Map<String, String> dataMap = new HashMap<>();
        dataMap.put("k1", "v1");
        dataMap.put("k2", "v2");
        Map<Integer, Exception> result = manager.batchCacheData(dataMap);
        assertTrue("batchCacheData成功时应返回空Map", result.isEmpty());
    }

    @Test
    public void batchCacheData_单存储器失败返回对应异常() {
        ICacheStorage<String, String> s1 = createStorage();
        ICacheStorage<String, String> s2 = createStorage();
        FaultInjectStorage<String, String> faultyStorage = new FaultInjectStorage<>(s2);
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(faultyStorage)
                .logger(new ConsoleLogger())
                .build();

        RuntimeException expectedEx = new RuntimeException("batchCacheData模拟写入失败");
        faultyStorage.exOnCacheData(expectedEx);

        Map<String, String> dataMap = new HashMap<>();
        dataMap.put("k1", "v1");
        Map<Integer, Exception> result = manager.batchCacheData(dataMap);
        assertEquals("batchCacheData应包含1个异常", 1, result.size());
        assertTrue("batchCacheData应包含索引1的异常", result.containsKey(1));
        assertSame("batchCacheData的异常应与预期一致", expectedEx, result.get(1));
    }

    // ==================== batchCache ====================

    @Test
    public void batchCache_成功时返回空Map() {
        ICacheStorage<String, String> s1 = createStorage();
        ICacheStorage<String, String> s2 = createStorage();
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(s2)
                .logger(new ConsoleLogger())
                .build();
        Map<String, String> dataMap = new HashMap<>();
        dataMap.put("k1", "v1");
        Map<String, RuntimeException> exMap = new HashMap<>();
        exMap.put("k2", new RuntimeException("err"));
        Map<Integer, Exception> result = manager.batchCache(dataMap, exMap);
        assertTrue("batchCache成功时应返回空Map", result.isEmpty());
    }

    @Test
    public void batchCache_单存储器失败返回对应异常() {
        ICacheStorage<String, String> s1 = createStorage();
        ICacheStorage<String, String> s2 = createStorage();
        FaultInjectStorage<String, String> faultyStorage = new FaultInjectStorage<>(s2);
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(faultyStorage)
                .logger(new ConsoleLogger())
                .build();

        RuntimeException expectedEx = new RuntimeException("batchCache模拟写入失败");
        faultyStorage.exOnCacheData(expectedEx);

        Map<String, String> dataMap = new HashMap<>();
        dataMap.put("k1", "v1");
        Map<Integer, Exception> result = manager.batchCache(dataMap, null);
        assertEquals("batchCache应包含1个异常", 1, result.size());
        assertTrue("batchCache应包含索引1的异常", result.containsKey(1));
        assertSame("batchCache的异常应与预期一致", expectedEx, result.get(1));
    }

    // ==================== invalidCache ====================

    @Test
    public void invalidCache_成功时返回空Map() {
        ICacheStorage<String, String> s1 = createStorage();
        ICacheStorage<String, String> s2 = createStorage();
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(s2)
                .logger(new ConsoleLogger())
                .build();
        // 先写入缓存，再失效
        manager.getDataPack("key");
        Map<Integer, Exception> result = manager.invalidCache("key");
        assertTrue("invalidCache成功时应返回空Map", result.isEmpty());
    }

    @Test
    public void invalidCache_单存储器失败返回对应异常() {
        ICacheStorage<String, String> s1 = createStorage();
        ICacheStorage<String, String> s2 = createStorage();
        FaultInjectStorage<String, String> faultyStorage = new FaultInjectStorage<>(s2);
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(faultyStorage)
                .logger(new ConsoleLogger())
                .build();

        RuntimeException expectedEx = new RuntimeException("invalidCache模拟失效失败");
        faultyStorage.exOnInvalidCache(expectedEx);

        Map<Integer, Exception> result = manager.invalidCache("key");
        assertEquals("invalidCache应包含1个异常", 1, result.size());
        assertTrue("invalidCache应包含索引1的异常", result.containsKey(1));
        assertSame("invalidCache的异常应与预期一致", expectedEx, result.get(1));
    }

    @Test
    public void invalidCache_指定索引其他索引不受影响() {
        ICacheStorage<String, String> s1 = createStorage();
        FaultInjectStorage<String, String> faultyS2 = new FaultInjectStorage<>(createStorage());
        ICacheStorage<String, String> s3 = createStorage();
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(faultyS2)
                .withCache(s3)
                .logger(new ConsoleLogger())
                .build();

        RuntimeException expectedEx = new RuntimeException("invalidCache模拟失败");
        faultyS2.exOnInvalidCache(expectedEx);

        // 只操作索引0和2，跳过索引1
        Map<Integer, Exception> result = manager.invalidCache("key", 0, 2);
        assertTrue("invalidCache跳过故障的索引1时应返回空Map", result.isEmpty());
    }

    // ==================== invalidAllCache ====================

    @Test
    public void invalidAllCache_成功时返回空Map() {
        ICacheStorage<String, String> s1 = createStorage();
        ICacheStorage<String, String> s2 = createStorage();
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(s2)
                .logger(new ConsoleLogger())
                .build();
        Map<Integer, Exception> result = manager.invalidAllCache();
        assertTrue("invalidAllCache成功时应返回空Map", result.isEmpty());
    }

    @Test
    public void invalidAllCache_单存储器失败返回对应异常() {
        ICacheStorage<String, String> s1 = createStorage();
        FaultInjectStorage<String, String> faultyStorage = new FaultInjectStorage<>(createStorage());
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(faultyStorage)
                .logger(new ConsoleLogger())
                .build();

        RuntimeException expectedEx = new RuntimeException("invalidAllCache模拟失效失败");
        faultyStorage.exOnInvalidAllCache(expectedEx);

        Map<Integer, Exception> result = manager.invalidAllCache();
        assertEquals("invalidAllCache应包含1个异常", 1, result.size());
        assertTrue("invalidAllCache应包含索引1的异常", result.containsKey(1));
        assertSame("invalidAllCache的异常应与预期一致", expectedEx, result.get(1));
    }

    // ==================== removeCache ====================

    @Test
    public void removeCache_成功时返回空Map() {
        ICacheStorage<String, String> s1 = createStorage();
        ICacheStorage<String, String> s2 = createStorage();
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(s2)
                .logger(new ConsoleLogger())
                .build();
        Map<Integer, Exception> result = manager.removeCache("key");
        assertTrue("removeCache成功时应返回空Map", result.isEmpty());
    }

    @Test
    public void removeCache_单存储器失败返回对应异常() {
        ICacheStorage<String, String> s1 = createStorage();
        ICacheStorage<String, String> s2 = createStorage();
        FaultInjectStorage<String, String> faultyStorage = new FaultInjectStorage<>(s2);
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(faultyStorage)
                .logger(new ConsoleLogger())
                .build();

        RuntimeException expectedEx = new RuntimeException("removeCache模拟移除失败");
        faultyStorage.exOnRemoveCache(expectedEx);

        Map<Integer, Exception> result = manager.removeCache("key");
        assertEquals("removeCache应包含1个异常", 1, result.size());
        assertTrue("removeCache应包含索引1的异常", result.containsKey(1));
        assertSame("removeCache的异常应与预期一致", expectedEx, result.get(1));
    }

    @Test
    public void removeCache_指定索引其他索引不受影响() {
        ICacheStorage<String, String> s1 = createStorage();
        FaultInjectStorage<String, String> faultyS2 = new FaultInjectStorage<>(createStorage());
        ICacheStorage<String, String> s3 = createStorage();
        DataManager<String, String> manager = DataManager.Builder
                .get("test", datasource)
                .withCache(s1)
                .withCache(faultyS2)
                .withCache(s3)
                .logger(new ConsoleLogger())
                .build();

        RuntimeException expectedEx = new RuntimeException("removeCache模拟失败");
        faultyS2.exOnRemoveCache(expectedEx);

        // 只操作索引0和2，跳过索引1（故障的存储）
        Map<Integer, Exception> result = manager.removeCache("key", 0, 2);
        assertTrue("removeCache跳过故障的索引1时应返回空Map", result.isEmpty());
    }
}
