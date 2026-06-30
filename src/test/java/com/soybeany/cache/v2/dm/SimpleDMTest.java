package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.exception.NoCacheException;
import com.soybeany.cache.v2.exception.NoDataSourceException;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.model.DataParam;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

import java.util.UUID;

/**
 * <br>Created by Soybeany on 2020/10/11.
 */
public class SimpleDMTest {

    private final IDatasource<String, String> datasource = s -> {
        System.out.println("“" + s + "”access datasource");
        return UUID.randomUUID().toString();
    };

    private final ICacheStorage<String, String> lruStorage = new LruMemCacheStorage.Builder<String, String>()
            .capacity(3)
            .pTtl(200)
            .build();

    private final DataManager<String, String> dataManager = DataManager.Builder
            .get("简单测试", datasource)
            .withCache(lruStorage)
            .logger(new ConsoleLogger())
            .build();

    @Test
    public void noLogger() {
        DataManager.Builder.get("无logger", datasource).withCache(lruStorage).build();
    }

    @Test
    public void sequenceTest() {
        String key = "key";
        // 第一次将访问数据源
        DataPack<String> data = dataManager.getDataPack(key);
        assert datasource.equals(data.provider) : "首次应访问数据源";
        // 第二次将读取lru
        data = dataManager.getDataPack(key);
        assert lruStorage.equals(data.provider) : "第二次应读取缓存";
    }

    @Test
    public void lruTest_容量限制淘汰最旧数据() throws Exception {
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";
        // 第一次均访问数据源
        assert datasource.equals(dataManager.getDataPack(key1).provider) : "key1首次应访问数据源";
        assert datasource.equals(dataManager.getDataPack(key2).provider) : "key2首次应访问数据源";
        assert datasource.equals(dataManager.getDataPack(key3).provider) : "key3首次应访问数据源";
        // 第二次均读取lru
        assert lruStorage.equals(dataManager.getDataPack(key2).provider) : "key2第二次应读取缓存";
        assert lruStorage.equals(dataManager.getDataPack(key3).provider) : "key3第二次应读取缓存";
        assert lruStorage.equals(dataManager.getDataPack(key1).provider) : "key1第二次应读取缓存";
        // 新增key则移除最旧的key（原先访问顺序中最早的key2被淘汰）
        String key4 = "key4";
        dataManager.getDataPack(key4);
        assert lruStorage.equals(dataManager.getDataPack(key4).provider) : "key4应读取缓存";
        assert lruStorage.equals(dataManager.getDataPack(key1).provider) : "key1应仍在缓存";
        assert lruStorage.equals(dataManager.getDataPack(key3).provider) : "key3应仍在缓存";
        // key2被淘汰，应访问数据源
        assert datasource.equals(dataManager.getDataPack(key2).provider) : "key2应被淘汰，重新访问数据源";
    }

    @Test
    public void concurrentTest_单发限制() throws Exception {
        int count = 10;
        final Object[] providers = new Object[count];
        Thread[] threads = new Thread[count];
        for (int i = 0; i < count; i++) {
            final int finalI = i;
            threads[i] = new Thread(() -> {
                DataPack<String> pack = dataManager.getDataPack(null);
                providers[finalI] = pack.provider;
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        // 并发限制：只能有一个线程访问数据源
        int accessCount = 0;
        for (Object provider : providers) {
            if (datasource == provider) {
                accessCount++;
            }
        }
        System.out.println("accessCount:" + accessCount);
        assert accessCount == 1 : "并发访问数据源次数应为1，实际: " + accessCount;
        // 后续直接读取缓存
        assert lruStorage == dataManager.getDataPack(null).provider : "并发后应读取缓存";
        // 等待缓存过期后再访问数据源
        Thread.sleep(200);
        assert datasource == dataManager.getDataPack(null).provider : "缓存过期后应重新访问数据源";
    }

    @Test
    public void specifyDatasourceTest() {
        final String source = "新数据源";
        String data = dataManager.getDataPack(null, s -> source).getData();
        assert source.equals(data) : "应使用指定的数据源";
    }

    @Test
    public void noDatasourceTest() {
        try {
            dataManager.getDataPack(null, null).getData();
            throw new Exception("不允许不抛出异常");
        } catch (Exception e) {
            assert e instanceof NoDataSourceException : "应抛出NoDataSourceException";
        }
    }

    @Test
    public void noCacheStorageTest() {
        DataManager<String, String> manager = DataManager.Builder.get("无缓存测试", datasource)
                .logger(new ConsoleLogger())
                .build();
        // 无缓存时每次都访问数据源
        assert datasource.equals(manager.getDataPack("key1").provider) : "无缓存首次应访问数据源";
        assert datasource.equals(manager.getDataPack("key1").provider) : "无缓存第二次也应访问数据源";
    }

    @Test
    public void removeKeyTest() {
        String key1 = "key1";
        String key2 = "key2";
        // 第一次均访问数据源
        assert datasource.equals(dataManager.getDataPack(key1).provider) : "key1首次应访问数据源";
        assert datasource.equals(dataManager.getDataPack(key2).provider) : "key2首次应访问数据源";
        // 移除key1的缓存
        dataManager.removeCache(key1);
        // key1应重新从数据源加载
        assert datasource.equals(dataManager.getDataPack(key1).provider) : "removeCache后key1应重新访问数据源";
        // key2不受影响，应从缓存读取
        assert lruStorage.equals(dataManager.getDataPack(key2).provider) : "key2应仍从缓存读取";
    }

    @Test
    public void containCache_无缓存时返回false() {
        assert !dataManager.containCache("non_exist") : "不存在的key应返回false";
    }

    @Test
    public void containCache_有缓存时返回true() {
        String key = "contain_test";
        dataManager.getDataPack(key);
        assert dataManager.containCache(key) : "已缓存的key应返回true";
    }
}
