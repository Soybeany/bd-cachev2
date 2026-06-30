package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.component.DBSimulationStorage;
import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.ICacheChecker;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.contract.user.IKeyConverter;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.exception.CacheWaitException;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

import java.util.List;

/**
 * 测试混合配置场景和缺失的API覆盖
 */
public class MixedConfigDMTest {

    private final IDatasource<String, String> datasource = s -> "数据:" + s;

    // ********************数据访问器方法********************

    @Test
    public void accessor_数据上下文不为空() {
        DataManager<String, String> manager = createManager(datasource);
        assert manager.dataContext() != null : "dataContext不应为null";
        assert "混合测试".equals(manager.dataContext().dataDesc) : "dataDesc应正确";
    }

    @Test
    public void accessor_默认数据源正确() {
        DataManager<String, String> manager = createManager(datasource);
        assert datasource == manager.defaultDatasource() : "defaultDatasource应与构建时一致";
    }

    @Test
    public void accessor_存储器列表正确() {
        ICacheStorage<String, String> l1 = new LruMemCacheStorage.Builder<String, String>().build();
        ICacheStorage<String, String> l2 = new DBSimulationStorage<>();
        DataManager<String, String> manager = DataManager.Builder
                .get("多级测试", datasource)
                .withCache(l1)
                .withCache(l2)
                .logger(new ConsoleLogger())
                .build();
        List<ICacheStorage<String, String>> storages = manager.storages();
        assert storages.size() == 2 : "应有2个存储器";
        assert storages.get(0) == l1 : "第一个应是l1";
        assert storages.get(1) == l2 : "第二个应是l2";
    }

    @Test
    public void accessor_续期标志正确() {
        DataManager<String, String> manager1 = DataManager.Builder
                .get("续期测试", datasource)
                .withCache(new LruMemCacheStorage.Builder<String, String>().build())
                .enableRenewExpiredCache(true)
                .logger(new ConsoleLogger())
                .build();
        assert manager1.enableRenewExpiredCache() : "enableRenewExpiredCache应为true";

        DataManager<String, String> manager2 = createManager(datasource);
        assert !manager2.enableRenewExpiredCache() : "默认enableRenewExpiredCache应为false";
    }

    // ********************storageId********************

    @Test
    public void storageId_自定义storageId正确传递() {
        DataManager<String, String> manager = DataManager.Builder
                .get("desc", datasource)
                .storageId("my-custom-id")
                .withCache(new LruMemCacheStorage.Builder<String, String>().build())
                .logger(new ConsoleLogger())
                .build();
        assert "my-custom-id".equals(manager.dataContext().storageId) : "storageId应正确设置到context";
    }

    @Test
    public void storageId_未设置时默认使用dataDesc() {
        DataManager<String, String> manager = DataManager.Builder
                .get("默认storageId", datasource)
                .withCache(new LruMemCacheStorage.Builder<String, String>().build())
                .logger(new ConsoleLogger())
                .build();
        assert "默认storageId".equals(manager.dataContext().storageId) : "未设置storageId时应使用dataDesc";
    }

    // ********************IKeyConverter (paramDesc / paramKey)********************

    @Test
    public void keyConverter_自定义paramDescConverter() {
        IKeyConverter<String> descConverter = s -> "前缀_" + s;
        DataManager<String, String> manager = DataManager.Builder
                .get("desc转换器", datasource, (IKeyConverter<String>) s -> s)
                .paramDescConverter(descConverter)
                .withCache(new LruMemCacheStorage.Builder<String, String>().build())
                .build();
        // 验证构建不报错，基本功能正常
        String data = manager.getData("test_key");
        assert "数据:test_key".equals(data) : "数据应正确获取";
    }

    @Test
    public void keyConverter_自定义paramKey影响缓存key() {
        // 所有key映射到同一个缓存key
        IKeyConverter<String> sameKeyConverter = s -> "same";
        DataManager<String, String> manager = DataManager.Builder
                .get("key转换器", datasource, sameKeyConverter)
                .withCache(new LruMemCacheStorage.Builder<String, String>().build())
                .build();
        // 不同param共享同一个缓存key，第一次访问数据源
        DataPack<String> pack1 = manager.getDataPack("key_a");
        assert datasource.equals(pack1.provider) : "首次应访问数据源";
        // 第二次访问应命中缓存（即使param不同，但缓存key相同）
        DataPack<String> pack2 = manager.getDataPack("key_b");
        assert pack2.provider instanceof ICacheStorage : "相同缓存key应命中缓存";
        assert pack2.getData().equals(pack1.getData()) : "相同缓存key应返回相同数据";
    }

    // ********************ttl / ttlErr********************

    @Test
    public void ttlConfig_秒级配置生效() throws Exception {
        // 使用ttl(秒)而非pTtl(毫秒)，验证秒级配置正常
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .ttl(1) // 1秒 = 1000ms
                .build();
        DataManager<String, String> manager = DataManager.Builder
                .get("ttl测试", datasource)
                .withCache(storage)
                .logger(new ConsoleLogger())
                .build();
        String key = "ttl_key";
        // 写入缓存
        manager.getDataPack(key);
        assert manager.getDataPack(key).provider instanceof ICacheStorage : "应命中缓存";
        // 等待超过1秒
        Thread.sleep(1100);
        // 缓存应过期
        assert datasource.equals(manager.getDataPack(key).provider) : "ttl过期后应访问数据源";
    }

    @Test
    public void ttlConfig_异常数据TTL配置() throws Exception {
        // 验证pTtlErr控制异常数据缓存时长
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .pTtlErr(500) // 异常数据500ms
                .build();
        IDatasource<String, String> normDs = s -> "正常数据";
        DataManager<String, String> manager = DataManager.Builder
                .get("ttlErr测试", normDs)
                .withCache(storage)
                .logger(new ConsoleLogger())
                .build();
        // 使用cacheException手动缓存异常（pTtl=Long.MAX_VALUE，受pTtlErr限制）
        manager.cacheException("err_key", new RuntimeException("模拟异常"));
        // 立即检查异常应已缓存
        DataPack<String> pack = manager.getDataPack("err_key");
        assert !pack.norm() : "异常应被缓存";
        assert storage.equals(pack.provider) : "应来自缓存";
        // 验证正常数据TTL（默认pTtl=1）和异常数据TTL不同
        manager.getDataPack("norm_key");
        DataPack<String> normPack = manager.getDataPack("norm_key");
        assert normPack.norm() : "正常数据应返回正常包";
        assert storage.equals(normPack.provider) : "正常数据应来自缓存";
    }

    // ********************deepCopy********************

    @Test
    public void deepCopy_每次读取返回不同对象() {
        ICacheStorage<String, MutableData> storage = new LruMemCacheStorage.Builder<String, MutableData>()
                .deepCopy(MutableData.class)
                .build();
        IDatasource<String, MutableData> ds = s -> new MutableData(s, 123);
        DataManager<String, MutableData> manager = DataManager.Builder
                .get("深拷贝测试", ds, (IKeyConverter<String>) s -> s)
                .withCache(storage)
                .logger(new ConsoleLogger())
                .build();
        String key = "deep_copy";
        // 第一次获取数据
        DataPack<MutableData> pack1 = manager.getDataPack(key);
        assert datasource != pack1.provider : "首次应访问...";
        // 第二次应从缓存读取，且与第一次是不同对象（深拷贝）
        DataPack<MutableData> pack2 = manager.getDataPack(key);
        assert pack2.provider instanceof ICacheStorage : "应从缓存读取";
        assert pack1.getData() != pack2.getData() : "deepCopy应返回不同对象";
        // 数据内容相同
        assert pack1.getData().value == pack2.getData().value : "数据值应相同";
    }

    // ********************enableDataCheck + 多级缓存********************

    @Test
    public void dataCheck_多级缓存场景() throws Exception {
        ICacheStorage<String, String> l1 = new LruMemCacheStorage.Builder<String, String>().pTtl(500).build();
        ICacheStorage<String, String> l2 = new DBSimulationStorage<>(60_000);
        int[] dsCount = {0};
        IDatasource<String, String> countingDs = s -> {
            dsCount[0]++;
            return "数据:" + s;
        };
        DataManager<String, String> manager = DataManager.Builder
                .get("dataCheck多级测试", countingDs)
                .withCache(l1)
                .withCache(l2)
                .enableDataCheck(p -> 200L, (param, pack) -> true)
                .logger(new ConsoleLogger())
                .build();
        String key = "dc_multi";
        // 第一次从数据源获取
        manager.getDataPack(key);
        assert dsCount[0] == 1 : "首次应访问数据源";
        // 第二次从l1获取（检查周期内）
        DataPack<String> pack2 = manager.getDataPack(key);
        assert l1.equals(pack2.provider) : "检查周期内应从l1获取";
        // 等待超过检查周期（200ms），触发检查 → needUpdate=true → 重新访问数据源
        Thread.sleep(250);
        DataPack<String> pack3 = manager.getDataPack(key);
        assert countingDs.equals(pack3.provider) : "检查周期外应重新访问数据源";
    }

    // ********************enableDataCheck + enableRenewExpiredCache********************

    @Test
    public void dataCheck_续期模式组合() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(300).build();
        int[] dsCount = {0};
        IDatasource<String, String> countingDs = s -> {
            dsCount[0]++;
            if (dsCount[0] == 2) {
                throw new RuntimeException("数据源异常");
            }
            return "数据:" + s;
        };
        DataManager<String, String> manager = DataManager.Builder
                .get("dataCheck+renew测试", countingDs)
                .withCache(storage)
                .enableDataCheck(p -> 150L, (param, pack) -> true)
                .enableRenewExpiredCache(true)
                .logger(new ConsoleLogger())
                .build();
        String key = "dc_renew";
        // 第一次获取（访问数据源）
        manager.getDataPack(key);
        assert dsCount[0] == 1 : "首次应访问数据源";
        // 等待检查周期 → 触发检查 → needUpdate → invalidCache → 重新访问数据源（第二次，抛异常）
        Thread.sleep(200);
        DataPack<String> pack = manager.getDataPack(key);
        // 因为开启了renew，数据源抛异常后应返回续期的旧数据
        assert pack.norm() : "renew开启时应返回续期的数据";
        assert ("数据:" + key).equals(pack.getData()) : "续期数据内容应正确";
        assert dsCount[0] == 2 : "应已进行第二次数据源访问";
    }

    // ********************datasourceTimeout + fallback********************

    @Test
    public void datasourceTimeout_fallback中数据源超时触发ExecutionException路径() {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>().pTtl(200).build();
        // datasourceTimeout很短（10ms），数据源很慢（3s），触发超时
        DataManager<String, String> manager = DataManager.Builder
                .get("dsTimeout+fallback测试", s -> {
                    try { Thread.sleep(3000); } catch (InterruptedException ignore) { }
                    return "ok";
                })
                .withCache(storage)
                .logger(new ConsoleLogger())
                .datasourceTimeout(10L)
                .build();
        // fallback调用时，异步getDataPack因datasourceTimeout快速失败，进入ExecutionException分支
        DataPack<String> pack = manager.getDataPackWithCacheFallback("no_cache_key");
        assert !pack.norm() : "无缓存且数据源超时时应返回异常包";
    }

    // ********************fetchLockTimeout + datasourceTimeout********************

    @Test
    public void fetchTimeout_datasourceTimeout共同配置() {
        // 两个超时都配置，验证各自独立工作
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .lockWaitTimeSingle(p -> 5000L)
                .lockWaitTimeAll(5000L)
                .pTtl(200)
                .build();
        DataManager<String, String> manager = DataManager.Builder
                .get("双重超时测试", s -> {
                    try { Thread.sleep(3000); } catch (InterruptedException ignore) { }
                    return "ok";
                })
                .withCache(storage)
                .logger(new ConsoleLogger())
                .datasourceTimeout(50L)     // 数据源超时50ms
                .fetchLockTimeout(p -> 5000L) // fetchLock超时5s（不影响datasourceTimeout）
                .build();
        // datasourceTimeout会先触发，返回CacheWaitException
        DataPack<String> pack = manager.getDataPack("key");
        assert !pack.norm() : "数据源应超时";
        assert pack.dataCore.exception instanceof CacheWaitException : "应抛出CacheWaitException";
    }

    // ********************containCache 更多场景********************

    @Test
    public void containCache_多级缓存时有效() {
        ICacheStorage<String, String> l1 = new LruMemCacheStorage.Builder<String, String>().pTtl(200).build();
        ICacheStorage<String, String> l2 = new DBSimulationStorage<>(60_000);
        DataManager<String, String> manager = DataManager.Builder
                .get("contain多级测试", datasource)
                .withCache(l1)
                .withCache(l2)
                .logger(new ConsoleLogger())
                .build();
        String key = "contain_multi";
        manager.getDataPack(key);
        assert manager.containCache(key) : "多级缓存中有数据时应返回true";
    }

    // ********************内部方法********************

    private DataManager<String, String> createManager(IDatasource<String, String> ds) {
        return DataManager.Builder
                .get("混合测试", ds)
                .withCache(new LruMemCacheStorage.Builder<String, String>().build())
                .logger(new ConsoleLogger())
                .build();
    }

    // 用于deepCopy测试的可变数据类
    @SuppressWarnings("unused")
    public static class MutableData {
        public String name;
        public int value;

        public MutableData() {
        }

        public MutableData(String name, int value) {
            this.name = name;
            this.value = value;
        }
    }
}
