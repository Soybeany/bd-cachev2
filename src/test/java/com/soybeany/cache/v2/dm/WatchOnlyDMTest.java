package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import com.soybeany.exception.BdRtException;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

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

    Holder<String> holder = new Holder<>(() -> {
        System.out.println("访问数据源");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return "ok";
    });

    @Test
    public void test() throws Exception {
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            futures.add(exe(holder, i));
        }
        Thread.sleep(1500);
        for (int i = 2; i < 10; i++) {
            futures.add(exe(holder, i));
        }
        for (Future<?> future : futures) {
            future.get();
        }
    }

    private static final ExecutorService SERVICE = Executors.newCachedThreadPool();

    private Future<?> exe(Holder<String> holder, int i) {
        return SERVICE.submit(() -> {
            try {
                holder.get("123", 2, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            System.out.println("结束:" + i);
        });
    }

    @After
    public void onClose() {
        holder.close();
    }

    private static class Holder<T> {
        private final ExecutorService service = Executors.newCachedThreadPool();
        private final Map<String, LockPair> locks = new WeakHashMap<>();
        private final Map<String, String> downloading = new ConcurrentHashMap<>();
        private final Map<String, T> result = new ConcurrentHashMap<>();
        private final Supplier<T> supplier;

        public Holder(Supplier<T> supplier) {
            this.supplier = supplier;
        }

        public T get(String key, long waitTimeout, TimeUnit unit) throws Exception {
            LockPair lockPair = getLockPair(key);
            try {
                // 若有缓存，直接使用
                T r = result.get(key);
                if (null != r) {
                    return r;
                }
                // 若还没请求数据源，则请求
                if (!downloading.containsKey(key)) {
                    downloading.put(key, "downloading");
                    service.submit(() -> {
                        try {
                            T t = supplier.get();
                            result.put(key, t);
                            LockPair lockPair2 = getLockPair(key);
                            try {
                                downloading.remove(key);
                                lockPair2.condition.signal();
                            } finally {
                                lockPair2.lock.unlock();
                            }
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                    });
                }
                // 等待数据
                boolean success = lockPair.condition.await(waitTimeout, unit);
                if (!success) {
                    throw new BdRtException("等待锁超时2");
                }
                // 通知下一个请求者解锁
                lockPair.condition.signal();
                // 获取数据
                return result.get(key);
            } finally {
                lockPair.lock.unlock();
            }
        }

        public void close() {
            service.shutdown();
        }

        private LockPair getLockPair(String key) throws Exception {
            LockPair lockPair;
            synchronized (locks) {
                lockPair = locks.computeIfAbsent(key, k -> new LockPair());
            }
            boolean success = lockPair.lock.tryLock(10, TimeUnit.SECONDS);
            if (!success) {
                throw new BdRtException("等待锁超时");
            }
            return lockPair;
        }

        private static class LockPair {
            final Lock lock = new ReentrantLock();
            final Condition condition = lock.newCondition();
        }
    }

    private void action(int i) {
        try {
            System.out.println(i + "开始");
            Thread.sleep(1000);
            System.out.println(i + "结束");
        } catch (Exception e) {
            System.out.println("err:" + e.getMessage());
        }
    }

}

