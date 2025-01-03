package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

public class WatchOnlyDMTest {

    ICacheStorage<String, String> cacheStorage = new LruMemCacheStorage.Builder<String, String>().pTtl(500).build();

    private final DataManager<String, String> dataManager = DataManager.Builder
            .get("包含测试", s -> "ok")
            .withCache(cacheStorage)
            .logger(new ConsoleLogger<>())
            .build();

    @Test
    public void containTest() {
        // 不存在的数据，查找时也不写入
        boolean contain = dataManager.containCache("1");
        assert !contain && cacheStorage.cachedDataCount(null) == 0;
        // 写入测试数据再试
        String key2 = "2";
        dataManager.getData(key2);
        assert dataManager.containCache(key2);
    }

}
