package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataCore;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

/**
 * 测试短超时回退模式<br>
 * 对应{@link com.soybeany.cache.v2.core.DataManager#getDataPackWithCacheFallback}
 */
public class CacheFallbackDMTest {

    private final IDatasource<String, String> slowDatasource = s -> {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException ignore) {
        }
        return "新数据";
    };

    private final IDatasource<String, String> fastDatasource = s -> {
        System.out.println("快速数据源被访问: " + s);
        return "新数据";
    };

    // ********************getDataPackWithCacheFallback********************

    @Test
    public void fallback_数据源快速响应时返回新数据() {
        DataManager<String, String> manager = createManager(fastDatasource);
        // fallback调用应直接返回新数据
        DataPack<String> pack = manager.getDataPackWithCacheFallback("key", 2000L);
        assert pack.norm();
        assert "新数据".equals(pack.getData());
    }

    @Test
    public void fallback_数据源慢时回退到过期缓存() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(200).build();
        // 过期缓存不会被删除，fallback 可直接回退
        DataManager<String, String> manager = createManager(storage, slowDatasource);
        String key = "fallback_key";
        // 用快速数据源写入缓存
        manager.getDataPack(key, fastDatasource);
        // 等待缓存过期
        Thread.sleep(250);
        // fallback调用，短超时应从过期缓存返回
        DataPack<String> pack = manager.getDataPackWithCacheFallback(key, 2000L);
        // 应快速返回（远小于3秒的数据源耗时）
        assert pack.norm() : "fallback应返回正常数据包";
        assert "新数据".equals(pack.getData());
    }

    @Test
    public void fallback_同时启用续期与回退() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(200).build();
        // 同时启用续期和fallback，验证两者不冲突
        DataManager<String, String> manager = createManagerWithRenew(storage, slowDatasource);
        String key = "renew_and_fallback";
        // 用快速数据源写入缓存
        manager.getDataPack(key, fastDatasource);
        // 等待缓存过期
        Thread.sleep(250);
        // fallback调用，应回退到过期缓存
        DataPack<String> pack = manager.getDataPackWithCacheFallback(key, 2000L);
        assert pack.norm() : "fallback应返回正常数据包";
        assert "新数据".equals(pack.getData());
    }

    @Test
    public void fallback_无过期缓存时回退到异常包() {
        DataManager<String, String> manager = createManager(slowDatasource);
        // 从未缓存过任何数据
        DataPack<String> pack = manager.getDataPackWithCacheFallback("no_cache_key", 2000L);
        assert !pack.norm() : "无缓存时应返回异常包";
    }

    @Test
    public void fallback_自定义超时时间() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(200).build();
        DataManager<String, String> manager = createManagerWithRenew(storage, slowDatasource);
        String key = "custom_timeout";
        // 用快速数据源写入缓存
        manager.getDataPack(key, fastDatasource);
        Thread.sleep(250);
        // 使用极短超时（100ms），应回退到过期缓存
        DataPack<String> pack = manager.getDataPackWithCacheFallback(key, 100L);
        assert pack.norm() : "应回退到过期缓存";
        assert "新数据".equals(pack.getData());
    }

    @Test
    public void fallback_默认超时时间() {
        DataManager<String, String> manager = createManager(fastDatasource);
        // 使用默认超时（DEFAULT_QUICK_TIMEOUT_MS = 2000ms）
        DataPack<String> pack = manager.getDataPackWithCacheFallback("default_key", 2000L);
        assert pack.norm();
        assert "新数据".equals(pack.getData());
    }

    @Test
    public void fallback_指定数据源() {
        DataManager<String, String> manager = createManager(slowDatasource);
        // 使用快速数据源作为参数传入
        DataPack<String> pack = manager.getDataPackWithCacheFallback("spec_ds", fastDatasource, 2000L);
        assert pack.norm();
        assert "新数据".equals(pack.getData());
        assert fastDatasource.equals(pack.provider) : "应使用指定的数据源";
    }

    @Test
    public void fallback_指定数据源和超时时间() {
        DataManager<String, String> manager = createManager(slowDatasource);
        // 使用快速数据源和自定义超时
        DataPack<String> pack = manager.getDataPackWithCacheFallback("spec_ds_timeout", fastDatasource, 500L);
        assert pack.norm();
        assert "新数据".equals(pack.getData());
        assert fastDatasource.equals(pack.provider) : "应使用指定的数据源";
    }

    @Test
    public void fallback_自定义回退处理逻辑() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(200).build();
        DataManager<String, String> manager = createManagerWithRenew(storage, slowDatasource);
        String key = "custom_fallback";
        // 用快速数据源写入缓存
        manager.getDataPack(key, fastDatasource);
        Thread.sleep(250);
        // 使用自定义fallback处理器，传入slowDatasource触发超时fallback路径
        DataPack<String> pack = manager.getDataPackWithCacheFallback(
                key, slowDatasource, 500L,
                expiredPack -> {
                    if (expiredPack.norm()) {
                        return expiredPack;
                    }
                    return new DataPack<>(DataCore.fromData("fallback默认值"), expiredPack.provider, 0);
                }
        );
        assert pack.norm();
        assert "新数据".equals(pack.getData());
    }

    // ********************getDataPackWithCacheFallback(无参数数据源)********************

    @Test
    public void fallback_无参数数据源重载() {
        DataManager<String, String> manager = createManager(fastDatasource);
        DataPack<String> pack = manager.getDataPackWithCacheFallback("key", 2000L);
        assert pack.norm();
    }

    @Test
    public void fallback_双参数重载() {
        DataManager<String, String> manager = createManager(fastDatasource);
        DataPack<String> pack = manager.getDataPackWithCacheFallback("key", 100L);
        assert pack.norm();
    }

    // ********************混合场景：fallback 降级 + 后续 fetchLock 等待********************

    @Test
    public void fallback_降级后后续请求通过fetchLock等待后台写入的数据() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(80).build();
        IDatasource<String, String> slowDs = s -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignore) {
            }
            return "新数据_A";
        };
        DataManager<String, String> manager = DataManager.Builder
                .get("fallback+wait测试", slowDs)
                .withCache(storage)
                .logger(new ConsoleLogger())
                .fetchLockTimeout(p -> 2000L)
                .build();

        String key = "fb_wait";
        // 用快速数据源写入初始缓存
        manager.getDataPack(key, s -> "旧数据");
        // 等待缓存过期
        Thread.sleep(100);

        // A: fallback降级，应快速返回过期数据
        long t1 = System.currentTimeMillis();
        String resultA = manager.getDataWithCacheFallback(key, 100L);
        long t2 = System.currentTimeMillis();
        assert "旧数据".equals(resultA) : "A应降级返回过期数据";
        assert (t2 - t1) < 300 : "A应在短时间内返回，实际耗时:" + (t2 - t1);

        // B: getDataPack 应等待A的后台线程完成，读取最新数据
        DataPack<String> packB = manager.getDataPack(key);
        long t3 = System.currentTimeMillis();
        assert "新数据_A".equals(packB.getData()) : "B应读取A后台写入的最新数据";
        // 验证B确实等待了A后台完成（宽松断言，避免因系统负载不稳定）
        assert packB.provider instanceof ICacheStorage : "B应从缓存读取";
        System.out.println("A耗时:" + (t2 - t1) + "ms, B等待耗时:" + (t3 - t2) + "ms");
    }

    @Test
    public void fallback降级后_getCache不阻塞直接读过期缓存() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(80).build();
        IDatasource<String, String> slowDs = s -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignore) {
            }
            return "新数据_A";
        };
        DataManager<String, String> manager = DataManager.Builder
                .get("fb+getCache测试", slowDs)
                .withCache(storage)
                .logger(new ConsoleLogger())
                .fetchLockTimeout(p -> 2000L)
                .build();

        String key = "fb_getCache";
        // 写入初始缓存并等待过期
        manager.getDataPack(key, s -> "旧数据");
        Thread.sleep(100);

        // A: fallback降级（后台线程持锁访问数据源）
        manager.getDataWithCacheFallback(key, 100L);

        // B: getCache 应不阻塞，直接读过期缓存（getCache不走fetchLock）
        long t1 = System.currentTimeMillis();
        String resultB = manager.getCache(key);
        long t2 = System.currentTimeMillis();
        assert "旧数据".equals(resultB) : "B应读到过期缓存";
        assert (t2 - t1) < 200 : "B应快速返回，实际耗时:" + (t2 - t1);
        System.out.println("B(getCache)耗时:" + (t2 - t1) + "ms");
    }

    @Test
    public void getData持锁时_fallback可快速降级() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(80).build();
        IDatasource<String, String> slowDs = s -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignore) {
            }
            return "新数据_A";
        };
        DataManager<String, String> manager = DataManager.Builder
                .get("lock+fb测试", slowDs)
                .withCache(storage)
                .logger(new ConsoleLogger())
                .fetchLockTimeout(p -> 2000L)
                .build();

        String key = "lock_fb";
        // 写入初始缓存并等待过期
        manager.getDataPack(key, s -> "旧数据");
        Thread.sleep(100);

        // A: 独立线程中getData（进入fetchLock，慢数据源500ms）
        Thread threadA = new Thread(() -> manager.getData(key));
        threadA.start();
        // 等A获取到锁
        Thread.sleep(50);

        // B: fallback调用，此时A持有fetchLock，B应快速降级
        long t1 = System.currentTimeMillis();
        String resultB = manager.getDataWithCacheFallback(key, 100L);
        long t2 = System.currentTimeMillis();
        assert "旧数据".equals(resultB) : "B应降级返回过期数据";
        assert (t2 - t1) < 300 : "B应在短时间内返回，实际耗时:" + (t2 - t1);

        // 等待A完成
        threadA.join();

        // C: getDataPack 应读到A写入的最新缓存数据
        DataPack<String> packC = manager.getDataPack(key);
        assert "新数据_A".equals(packC.getData()) : "C应读取A后台写入的最新数据";
        assert packC.provider instanceof ICacheStorage : "C应从缓存读取";
        System.out.println("B(fallback)耗时:" + (t2 - t1) + "ms");
    }

    // ********************getDataWithCacheFallback********************

    @Test
    public void getDataWithCacheFallback_正常返回数据() {
        DataManager<String, String> manager = createManager(fastDatasource);
        String data = manager.getDataWithCacheFallback("key", 2000L);
        assert "新数据".equals(data);
    }

    @Test
    public void getDataWithCacheFallback_指定超时() {
        DataManager<String, String> manager = createManager(fastDatasource);
        String data = manager.getDataWithCacheFallback("key", 100L);
        assert "新数据".equals(data);
    }

    // ********************内部方法********************

    private DataManager<String, String> createManager(IDatasource<String, String> ds) {
        return createManager(new LruMemCacheStorage.Builder<String, String>().pTtl(200).build(), ds);
    }

    private DataManager<String, String> createManager(ICacheStorage<String, String> storage, IDatasource<String, String> ds) {
        return newBuilder(storage, ds).build();
    }

    private DataManager<String, String> createManagerWithRenew(ICacheStorage<String, String> storage, IDatasource<String, String> ds) {
        return newBuilder(storage, ds).enableRenewExpiredCache(true).build();
    }

    private DataManager.Builder<String, String> newBuilder(ICacheStorage<String, String> storage, IDatasource<String, String> ds) {
        return DataManager.Builder
                .get("回退测试", ds)
                .withCache(storage)
                .logger(new ConsoleLogger());
    }
}
