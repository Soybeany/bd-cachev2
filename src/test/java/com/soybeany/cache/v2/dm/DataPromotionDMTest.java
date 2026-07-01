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
 * 测试多级缓存中的数据提升(Data Promotion)行为，以及needStore=false模式<br>
 * 对应{@link com.soybeany.cache.v2.core.DataManager#getDataPack} 中数据回写(数据提升)逻辑
 */
public class DataPromotionDMTest {

    private final IDatasource<String, String> datasource = s -> {
        System.out.println("访问数据源: " + s);
        return "数据源值:" + s;
    };

    // ********************数据提升(Data Promotion)********************

    @Test
    public void promotion_从二级缓存读取后自动提升到一级缓存() throws Exception {
        ICacheStorage<String, String> l1 = new LruMemCacheStorage.Builder<String, String>().pTtl(200).build();
        ICacheStorage<String, String> l2 = new DBSimulationStorage<>(60_000);
        DataManager<String, String> manager = DataManager.Builder
                .get("数据提升测试", datasource)
                .withCache(l1)
                .withCache(l2)
                .logger(new ConsoleLogger())
                .build();
        String key = "promotion_key";
        // 第一次从数据源获取，数据应写入l2(l1也会写入)
        DataPack<String> pack1 = manager.getDataPack(key);
        assert datasource.equals(pack1.provider);
        // 等待l1过期
        Thread.sleep(250);
        // 此时l1已过期，l2仍有数据，应从l2获取
        DataPack<String> pack2 = manager.getDataPack(key);
        assert l2.equals(pack2.provider) : "应从l2获取数据，实际: " + pack2.provider;
        // 由于数据提升，l1应该也写入了数据
        DataPack<String> pack3 = manager.getDataPack(key);
        assert l1.equals(pack3.provider) : "数据提升后应从l1获取数据，实际: " + pack3.provider;
    }

    @Test
    public void promotion_三级缓存逐级提升() throws Exception {
        ICacheStorage<String, String> l1 = new LruMemCacheStorage.Builder<String, String>().pTtl(200).build();
        ICacheStorage<String, String> l2 = new LruMemCacheStorage.Builder<String, String>().pTtl(400).build();
        ICacheStorage<String, String> l3 = new DBSimulationStorage<>(60_000);
        DataManager<String, String> manager = DataManager.Builder
                .get("三级提升测试", datasource)
                .withCache(l1)
                .withCache(l2)
                .withCache(l3)
                .logger(new ConsoleLogger())
                .build();
        String key = "promotion_3";
        // 从数据源获取
        manager.getDataPack(key);
        // 等待l1,l2都过期
        Thread.sleep(450);
        // 应从l3获取
        DataPack<String> pack1 = manager.getDataPack(key);
        assert l3.equals(pack1.provider) : "应从l3获取，实际: " + pack1.provider;
        // 数据应被提升到l2,l1
        DataPack<String> pack2 = manager.getDataPack(key);
        assert l1.equals(pack2.provider) : "数据提升后应从l1获取，实际: " + pack2.provider;
    }

    @Test
    public void promotion_缓存未过期时直接从当前层读取() throws Exception {
        ICacheStorage<String, String> l1 = new LruMemCacheStorage.Builder<String, String>().pTtl(60_000).build();
        ICacheStorage<String, String> l2 = new DBSimulationStorage<>(60_000);
        DataManager<String, String> manager = DataManager.Builder
                .get("提升监控测试", datasource)
                .withCache(l1)
                .withCache(l2)
                .logger(new ConsoleLogger())
                .build();
        String key = "promotion_monitor";
        // 第一次从数据源获取，l1和l2都有缓存
        manager.getDataPack(key);
        // 第二次l1未过期，直接从l1读取，不经过l2
        DataPack<String> pack = manager.getDataPack(key);
        assert l1.equals(pack.provider) : "缓存未过期时应直接从l1读取，实际: " + pack.provider;
    }

    // ********************needStore=false********************

    @Test
    public void needStoreFalse_获取数据但不缓存() {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(60_000).build();
        DataManager<String, String> manager = DataManager.Builder
                .get("needStore测试", datasource)
                .withCache(storage)
                .logger(new ConsoleLogger())
                .build();
        String key = "no_store_key";
        // 使用needStore=false获取数据
        DataPack<String> pack = manager.getDataPack(key, datasource, false);
        assert datasource.equals(pack.provider);
        assert "数据源值:no_store_key".equals(pack.getData());
        // 缓存中不应有数据
        DataPack<String> pack2 = manager.getDataPack(key);
        assert datasource.equals(pack2.provider) : "needStore=false后，获取数据不应被缓存，应再次访问数据源";
    }

    @Test
    public void needStoreFalse_多级缓存也不缓存() {
        ICacheStorage<String, String> l1 = new LruMemCacheStorage.Builder<String, String>().pTtl(60_000).build();
        ICacheStorage<String, String> l2 = new DBSimulationStorage<>(60_000);
        DataManager<String, String> manager = DataManager.Builder
                .get("needStore多级测试", datasource)
                .withCache(l1)
                .withCache(l2)
                .logger(new ConsoleLogger())
                .build();
        String key = "no_store_multi";
        // 使用needStore=false获取数据
        DataPack<String> first = manager.getDataPack(key, datasource, false);
        assert datasource.equals(first.provider);
        // 不应被L1或L2缓存
        DataPack<String> second = manager.getDataPack(key);
        assert datasource.equals(second.provider) : "needStore=false后，数据不应被任何存储层缓存";
    }

    @Test
    public void needStoreTrue_默认缓存行为() {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(60_000).build();
        DataManager<String, String> manager = DataManager.Builder
                .get("needStore默认测试", datasource)
                .withCache(storage)
                .logger(new ConsoleLogger())
                .build();
        String key = "store_key";
        // 使用默认needStore=true获取数据
        DataPack<String> first = manager.getDataPack(key);
        assert datasource.equals(first.provider);
        // 缓存中应有数据
        DataPack<String> second = manager.getDataPack(key);
        assert storage.equals(second.provider) : "默认needStore=true，数据应被缓存";
    }

}
