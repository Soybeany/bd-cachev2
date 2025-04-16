package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.exception.CacheWaitException;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


public class CacheWaitTest {

    private final IDatasource<String, String> datasource = s -> {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ignore) {
        }
        return "ok";
    };

    private final DataManager<String, String> dataManager = DataManager.Builder
            .get("锁测试", datasource)
            .withCache(new LruMemCacheStorage.Builder<String, String>().pTtl(200).build())
            .lockWaitTime(1)
            .logger(new ConsoleLogger())
            .build();

    @Test
    public void concurrentTest2() throws Exception {
        int count = 5;
        List<CacheWaitException> exceptions = new ArrayList<>();
        Thread[] threads = new Thread[count];
        for (int i = 0; i < count; i++) {
            threads[i] = new Thread(() -> {
                try {
                    dataManager.getData(null);
                } catch (CacheWaitException e) {
                    exceptions.add(e);
                }
            });
            threads[i].start();
            Thread.sleep(310);
        }
        for (Thread thread : threads) {
            thread.join();
        }
        // 单发限制
        System.out.println("exceptionCount:" + exceptions.size());
        assert exceptions.size() == 3;
    }
}
