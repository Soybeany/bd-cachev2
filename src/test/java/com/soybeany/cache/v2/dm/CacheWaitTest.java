package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.exception.CacheWaitException;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;


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
            .withCache(new TestStorage<>(200))
            .logger(new ConsoleLogger<>())
            .build();

    @Test
    public void concurrentTest2() throws Exception {
        int count = 10;
        final Object[] exceptions = new Object[count];
        Thread[] threads = new Thread[count];
        for (int i = 0; i < count; i++) {
            final int finalI = i;
            threads[i] = new Thread(() -> {
                DataPack<String> pack = dataManager.getDataPack(null);
                exceptions[finalI] = pack.dataCore.exception;
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        // 并发限制
        int exceptionCount = 0;
        for (Object exception : exceptions) {
            if (exception instanceof CacheWaitException) {
                exceptionCount++;
            }
        }
        // 单发限制
        System.out.println("exceptionCount:" + exceptionCount);
        assert exceptionCount == 9;
    }

    private static class TestStorage<Param, Data> extends LruMemCacheStorage<Param, Data> {

        public TestStorage(int pTtl) {
            super(pTtl, 60 * 1000, 100);
        }

        @Override
        public long lockWaitTime() {
            return 1;
        }
    }

}
