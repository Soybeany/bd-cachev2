package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

import java.util.UUID;

/**
 * <br>Created by Soybeany on 2020/11/17.
 */
public class MultiKeyDMTest {

    private final ICacheStorage<String, String> lruStorage = new LruMemCacheStorage.Builder<String, String>().pTtl(800).build();

    private final DataManager<String, String> dataManager = DataManager.Builder
            .get("MultiKey", s -> {
                System.out.println("“" + s + "”access datasource");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException("休眠中断:" + e.getMessage());
                }
                return s;
            })
            .withCache(lruStorage)
            .logger(new ConsoleLogger())
            .build();

    @Test
    public void test_不同key并行访问不串行() throws Exception {
        int count = 10;
        Thread[] threads = new Thread[count];
        for (int i = 0; i < count; i++) {
            threads[i] = new Thread(() -> {
                String key = UUID.randomUUID().toString();
                dataManager.getDataPack(key);
            });
            threads[i].start();
        }
        long start = System.currentTimeMillis();
        for (Thread thread : threads) {
            thread.join();
        }
        long delta = System.currentTimeMillis() - start;
        System.out.println("时差:" + delta);
        // 不同key可以并行访问数据源，整体执行时间应远小于 count * 500ms
        assert delta < 2000 : "并行耗时应小于2000ms，实际: " + delta;
    }
}
