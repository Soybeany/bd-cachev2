package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.exception.CacheWaitException;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import com.soybeany.cache.v2.storage.ReentrantLockSupport;
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
            .withCache(new TestStorage<>())
            .logger(new ConsoleLogger())
            .build();

    @Test
    public void concurrentTest() throws Exception {
        onExe(5, 3, 0, 310);
    }

    @Test
    public void concurrentTest2() throws Exception {
        new Thread(dataManager::invalidAllCache).start();
        onExe(2, 1, 75, 10);
    }

    private void onExe(int exeCount, int assertCount, int initSleepTime, int roundSleepTime) throws Exception {
        Thread.sleep(initSleepTime);
        List<CacheWaitException> exceptions = new ArrayList<>();
        Thread[] threads = new Thread[exeCount];
        for (int i = 0; i < exeCount; i++) {
            threads[i] = new Thread(() -> {
                try {
                    dataManager.getData(null);
                } catch (CacheWaitException e) {
                    exceptions.add(e);
                }
            });
            threads[i].start();
            // 逐渐累加
            Thread.sleep(roundSleepTime);
        }
        for (Thread thread : threads) {
            thread.join();
        }
        System.out.println("exceptionCount:" + exceptions.size());
        assert exceptions.size() == assertCount;
    }

    private static class TestStorage<Param, Data> extends LruMemCacheStorage<Param, Data> {

        public TestStorage() {
            super(200, 60 * 1000, 100, new ReentrantLockSupport<>(p -> 1000L, 20L));
        }

        @Override
        public synchronized void onInvalidAllCache() {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            super.onInvalidAllCache();
        }
    }
}
