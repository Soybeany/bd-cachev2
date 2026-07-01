package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.exception.CacheWaitException;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import com.soybeany.cache.v2.storage.StdKeyLock;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 测试缓存等待/并发场景
 * <br>对应fetchLock超时机制
 */
public class CacheWaitTest {

    private final IDatasource<String, String> slowDatasource = s -> {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ignore) {
        }
        return "ok";
    };

    @Test
    public void concurrentTest() throws Exception {
        // 使用快速多次并发访问，验证fetchLock的去重效果
        int count = 5;
        List<Exception> exceptions = new ArrayList<>();
        DataManager<String, String> manager = createManager(slowDatasource, 1000L);
        Thread[] threads = new Thread[count];
        for (int i = 0; i < count; i++) {
            threads[i] = new Thread(() -> {
                try {
                    manager.getData("key");
                } catch (CacheWaitException e) {
                    System.out.println("线程" + Thread.currentThread().getName() + "遇到CacheWaitException: " + e.getMessage());
                    exceptions.add(e);
                } catch (Exception e) {
                    System.out.println("线程" + Thread.currentThread().getName() + "遇到其他异常: " + e.getMessage());
                }
            });
            threads[i].start();
            Thread.sleep(310);
        }
        for (Thread thread : threads) {
            thread.join();
        }
        System.out.println("exceptionCount:" + exceptions.size());
        // 第一批线程获取锁成功，后续线程可能因锁超时而失败，但不一定全部失败
        // 至少应该有部分线程因为fetchLock超时而抛出CacheWaitException
        assert exceptions.size() > 0 : "部分线程应因fetchLock超时而抛出异常";
    }

    @Test
    public void fastDatasourceNoTimeout() throws Exception {
        IDatasource<String, String> fastDs = s -> "ok";
        DataManager<String, String> manager = createManager(fastDs, 1000L);
        int count = 5;
        Thread[] threads = new Thread[count];
        for (int i = 0; i < count; i++) {
            threads[i] = new Thread(() -> manager.getData("key"));
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        // 快速数据源不应有锁超时
        String data = manager.getData("key");
        assert "ok".equals(data);
    }

    private DataManager<String, String> createManager(IDatasource<String, String> ds, long fetchLockTimeoutMs) {
        return DataManager.Builder
                .get("锁测试", ds)
                .withCache(new LruMemCacheStorage.Builder<String, String>().build())
                .logger(new ConsoleLogger())
                .fetchLock(new StdKeyLock("fetch", p -> fetchLockTimeoutMs))
                .build();
    }
}
