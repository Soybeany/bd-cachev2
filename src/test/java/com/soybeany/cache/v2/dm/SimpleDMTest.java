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
        assert datasource.equals(data.provider);
        // 第二次将读取lru
        data = dataManager.getDataPack(key);
        assert lruStorage.equals(data.provider);
    }

    @Test
    public void lruTest() throws Exception {
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";
        // 第一次均访问数据源
        DataPack<String> data = dataManager.getDataPack(key1);
        assert datasource.equals(data.provider);
        data = dataManager.getDataPack(key2);
        assert datasource.equals(data.provider);
        data = dataManager.getDataPack(key3);
        assert datasource.equals(data.provider);
        // 第二次均读取lru
        data = dataManager.getDataPack(key2);
        assert lruStorage.equals(data.provider);
        data = dataManager.getDataPack(key3);
        assert lruStorage.equals(data.provider);
        data = dataManager.getDataPack(key1);
        assert lruStorage.equals(data.provider);
        // 新增key则移除最旧的key
        String key4 = "key4";
        dataManager.getDataPack(key4);
        lruStorage.onGetCache(new DataParam<>(null, key3, null));
        try {
            lruStorage.onGetCache(new DataParam<>(null, key2, null));
            throw new Exception("不允许还持有缓存");
        } catch (NoCacheException e) {
            System.out.println("“" + key2 + "”的缓存已移除");
        }
    }

    @Test
    public void concurrentTest() throws Exception {
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
        // 并发限制
        int accessCount = 0;
        for (Object provider : providers) {
            if (datasource == provider) {
                accessCount++;
            }
        }
        // 单发限制
        System.out.println("accessCount:" + accessCount);
        assert accessCount == 1;
        DataPack<String> pack1 = dataManager.getDataPack(null);
        assert lruStorage == pack1.provider;
        Thread.sleep(200);
        DataPack<String> pack2 = dataManager.getDataPack(null);
        assert datasource == pack2.provider;
    }

    @Test
    public void specifyDatasourceTest() throws Exception {
        final String source = "新数据源";
        String data = dataManager.getDataPack(null, s -> source).getData();
        assert source.equals(data);
    }

    @Test
    public void noDatasourceTest() {
        try {
            dataManager.getDataPack(null, null).getData();
            throw new Exception("不允许不抛出异常");
        } catch (Exception e) {
            assert e instanceof NoDataSourceException;
        }
    }

    @Test
    public void noCacheStorageTest() {
        DataManager<String, String> manager = DataManager.Builder.get("无缓存测试", datasource)
                .logger(new ConsoleLogger())
                .build();
        DataPack<String> pack = manager.getDataPack("key1");
        assert datasource.equals(pack.provider);
        pack = manager.getDataPack("key1");
        assert datasource.equals(pack.provider);
    }

    @Test
    public void removeKeyTest() {
        String key1 = "key1";
        String key2 = "key2";
        // 第一次将访问1数据源
        DataPack<String> data = dataManager.getDataPack(key1);
        assert datasource.equals(data.provider);
        // 第一次将访问2数据源
        DataPack<String> data2 = dataManager.getDataPack(key2);
        assert datasource.equals(data2.provider);
        // 移除1的缓存，1将重新从数据源加载
        dataManager.removeCache(key1, 0);
        data = dataManager.getDataPack(key1);
        assert datasource.equals(data.provider);
        // 2不受影响
        data2 = dataManager.getDataPack(key2);
        assert lruStorage.equals(data2.provider);
    }

}
