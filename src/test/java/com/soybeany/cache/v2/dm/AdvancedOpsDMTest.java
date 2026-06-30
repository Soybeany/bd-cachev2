package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.component.DBSimulationStorage;
import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

/**
 * 测试高级存储操作：getDataPackDirectly正常场景、clearCache指定索引、removeCache指定索引
 */
public class AdvancedOpsDMTest {

    private final IDatasource<String, String> datasource = s -> {
        System.out.println("访问数据源: " + s);
        return "数据:" + s;
    };

    // ********************getDataPackDirectly 正常场景********************

    @Test
    public void directly_直接获取数据源数据() {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(60_000).build();
        DataManager<String, String> manager = DataManager.Builder
                .get("directly测试", datasource)
                .withCache(storage)
                .logger(new ConsoleLogger())
                .build();
        String key = "direct_key";
        // 直接从数据源获取（绕过缓存）
        DataPack<String> pack = manager.getDataPackDirectly(key);
        assert datasource.equals(pack.provider);
        assert "数据:direct_key".equals(pack.getData());
        // 缓存中不应有数据（getDataPackDirectly不写缓存）
        DataPack<String> pack2 = manager.getDataPack(key);
        assert datasource.equals(pack2.provider) : "getDataPackDirectly不应写入缓存";
    }

    @Test
    public void directly_多次调用互不影响() {
        DataManager<String, String> manager = DataManager.Builder
                .get("directly多次测试", datasource)
                .logger(new ConsoleLogger())
                .build();
        // 多次调用直接获取
        DataPack<String> p1 = manager.getDataPackDirectly("k1");
        DataPack<String> p2 = manager.getDataPackDirectly("k2");
        assert "数据:k1".equals(p1.getData());
        assert "数据:k2".equals(p2.getData());
    }

    // ********************clearCache 指定存储器********************

    @Test
    public void clearCache_清除所有存储器() {
        ICacheStorage<String, String> l1 = new LruMemCacheStorage.Builder<String, String>().pTtl(60_000).build();
        ICacheStorage<String, String> l2 = new DBSimulationStorage<>(60_000);
        DataManager<String, String> manager = DataManager.Builder
                .get("clearAll测试", datasource)
                .withCache(l1)
                .withCache(l2)
                .logger(new ConsoleLogger())
                .build();
        String key = "clear_all";
        // 写入缓存
        manager.getDataPack(key);
        // 清除所有缓存
        manager.clearCache();
        // 应再次从数据源获取
        DataPack<String> pack = manager.getDataPack(key);
        assert datasource.equals(pack.provider) : "clearCache后应从数据源获取";
    }

    @Test
    public void clearCache_清除指定存储器() {
        ICacheStorage<String, String> l1 = new LruMemCacheStorage.Builder<String, String>().pTtl(200).build();
        ICacheStorage<String, String> l2 = new DBSimulationStorage<>(60_000);
        DataManager<String, String> manager = DataManager.Builder
                .get("clear指定测试", datasource)
                .withCache(l1)
                .withCache(l2)
                .logger(new ConsoleLogger())
                .build();
        String key = "clear_single";
        // 写入缓存
        manager.getDataPack(key);
        // 只清除l1（索引0）
        manager.clearCache(0);
        // l1已清除，应从l2获取
        DataPack<String> pack2 = manager.getDataPack(key);
        assert l2.equals(pack2.provider) : "clearCache(0)后应从l2获取，实际: " + pack2.provider;
    }

    @Test
    public void clearCache_清除多个指定存储器() {
        ICacheStorage<String, String> l1 = new LruMemCacheStorage.Builder<String, String>().pTtl(200).build();
        ICacheStorage<String, String> l2 = new DBSimulationStorage<>(60_000);
        DataManager<String, String> manager = DataManager.Builder
                .get("clear多个测试", datasource)
                .withCache(l1)
                .withCache(l2)
                .logger(new ConsoleLogger())
                .build();
        String key = "clear_multi";
        // 写入缓存
        manager.getDataPack(key);
        // 同时清除l1和l2
        manager.clearCache(0, 1);
        // 应从数据源获取
        DataPack<String> pack = manager.getDataPack(key);
        assert datasource.equals(pack.provider) : "clearCache(0,1)后应从数据源获取";
    }

    // ********************removeCache 指定存储器********************

    @Test
    public void removeCache_移除指定存储器中的缓存() {
        ICacheStorage<String, String> l1 = new LruMemCacheStorage.Builder<String, String>().pTtl(60_000).build();
        ICacheStorage<String, String> l2 = new DBSimulationStorage<>(60_000);
        DataManager<String, String> manager = DataManager.Builder
                .get("remove测试", datasource)
                .withCache(l1)
                .withCache(l2)
                .logger(new ConsoleLogger())
                .build();
        String key1 = "remove_k1";
        String key2 = "remove_k2";
        // 写入缓存
        manager.getDataPack(key1);
        manager.getDataPack(key2);
        // 移除key1在l1中的缓存
        manager.removeCache(key1, 0);
        // key1应从l2获取（而非数据源）
        DataPack<String> pack1 = manager.getDataPack(key1);
        assert l2.equals(pack1.provider) : "removeCache(key1,0)后key1应从l2获取";
        // key2不受影响，应从l1获取
        DataPack<String> pack2 = manager.getDataPack(key2);
        assert l1.equals(pack2.provider) : "key2不应受影响";
    }

    @Test
    public void removeCache_移除所有存储器中的缓存() {
        ICacheStorage<String, String> l1 = new LruMemCacheStorage.Builder<String, String>().pTtl(60_000).build();
        ICacheStorage<String, String> l2 = new DBSimulationStorage<>(60_000);
        DataManager<String, String> manager = DataManager.Builder
                .get("removeAll测试", datasource)
                .withCache(l1)
                .withCache(l2)
                .logger(new ConsoleLogger())
                .build();
        String key = "remove_all";
        // 写入缓存
        manager.getDataPack(key);
        // 不指定索引移除所有
        manager.removeCache(key);
        // 应从数据源获取
        DataPack<String> pack = manager.getDataPack(key);
        assert datasource.equals(pack.provider) : "removeCache(key)后应从数据源获取";
    }

    @Test
    public void removeCache_不存在的key不报错() {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(60_000).build();
        DataManager<String, String> manager = DataManager.Builder
                .get("remove不存在测试", datasource)
                .withCache(storage)
                .logger(new ConsoleLogger())
                .build();
        // 移除不存在的key不应抛异常
        manager.removeCache("non_existent_key");
    }

    // ********************invalidCache 指定存储器********************

    @Test
    public void invalidCache_失效指定存储器中的缓存() throws Exception {
        ICacheStorage<String, String> l1 = new LruMemCacheStorage.Builder<String, String>().pTtl(60_000).build();
        ICacheStorage<String, String> l2 = new DBSimulationStorage<>(60_000);
        DataManager<String, String> manager = DataManager.Builder
                .get("invalid指定测试", datasource)
                .withCache(l1)
                .withCache(l2)
                .logger(new ConsoleLogger())
                .enableRenewExpiredCache(true)
                .build();
        String key = "invalid_single";
        // 写入缓存
        manager.getDataPack(key);
        // 只失效l1中的缓存（索引0）
        manager.invalidCache(key, 0);
        // 由于开启了续期且l2有数据，应从l2获取
        DataPack<String> pack = manager.getDataPack(key);
        assert l2.equals(pack.provider) : "invalidCache(key,0)后应从l2获取";
    }

    // ********************invalidAllCache 指定存储器********************

    @Test
    public void invalidAllCache_失效指定存储器() {
        ICacheStorage<String, String> l1 = new LruMemCacheStorage.Builder<String, String>().pTtl(60_000).build();
        ICacheStorage<String, String> l2 = new DBSimulationStorage<>(60_000);
        DataManager<String, String> manager = DataManager.Builder
                .get("invalidAll测试", datasource)
                .withCache(l1)
                .withCache(l2)
                .logger(new ConsoleLogger())
                .build();
        String key = "invalid_all";
        // 写入缓存
        manager.getDataPack(key);
        // 只失效l1
        manager.invalidAllCache(0);
        // 应从l2获取
        DataPack<String> pack = manager.getDataPack(key);
        assert l2.equals(pack.provider) : "invalidAllCache(0)后应从l2获取";
    }
}
