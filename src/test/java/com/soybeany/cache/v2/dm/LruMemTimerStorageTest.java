package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

import java.util.UUID;

/**
 * 测试LRU存储的过期和淘汰行为
 *
 * @author Soybeany
 * @date 2021/2/20
 */
public class LruMemTimerStorageTest {

    IDatasource<String, String> datasource = s -> {
        System.out.println("“" + s + "”access datasource");
        return UUID.randomUUID().toString();
    };

    ICacheStorage<String, String> cacheStorage = new LruMemCacheStorage.Builder<String, String>().capacity(3).pTtl(500).build();

    private final DataManager<String, String> dataManager = DataManager.Builder
            .get("LRU存储器测试", datasource)
            .withCache(cacheStorage)
            .logger(new ConsoleLogger())
            .build();

    @Test
    public void test_缓存过期后重新访问数据源() throws Exception {
        String key = "key1";
        // 第一次从数据源获取
        DataPack<String> pack = dataManager.getDataPack(key);
        assert datasource.equals(pack.provider) : "首次应访问数据源";
        // 第二次从缓存读取
        pack = dataManager.getDataPack(key);
        assert cacheStorage.equals(pack.provider) : "第二次应从缓存读取";
        // 等待缓存过期
        Thread.sleep(600);
        // 再次访问数据源
        pack = dataManager.getDataPack(key);
        assert datasource.equals(pack.provider) : "缓存过期后应重新访问数据源";
    }

    @Test
    public void test_LRU淘汰最旧数据() throws Exception {
        String key1 = "k_lru_1";
        String key2 = "k_lru_2";
        String key3 = "k_lru_3";
        // 写入3个key
        dataManager.getDataPack(key1);
        dataManager.getDataPack(key2);
        dataManager.getDataPack(key3);
        // 验证均从缓存读取
        assert cacheStorage.equals(dataManager.getDataPack(key1).provider) : "key1应缓存";
        assert cacheStorage.equals(dataManager.getDataPack(key2).provider) : "key2应缓存";
        assert cacheStorage.equals(dataManager.getDataPack(key3).provider) : "key3应缓存";
        // 新增第4个key触发LRU淘汰（capacity=3）
        String key4 = "k_lru_4";
        dataManager.getDataPack(key4);
        // key1应被淘汰（最早写入）
        assert datasource.equals(dataManager.getDataPack(key1).provider) : "key1应被LRU淘汰";
    }

}
