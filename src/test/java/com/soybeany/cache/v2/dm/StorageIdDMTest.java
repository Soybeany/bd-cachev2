package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.contract.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

/**
 * @author Soybeany
 * @date 2022/2/10
 */
public class StorageIdDMTest {

    IDatasource<String, String> source = s -> "成功";
    ICacheStorage<String, String> lru1 = new LruMemCacheStorage.Builder<String, String>().enableShareStorage(true).build();
    private final DataManager<String, String> dataManager1 = DataManager.Builder
            .get("简单测试", source)
            .withCache(lru1)
            .logger(new ConsoleLogger<>())
            .storageId("sameId")
            .build();

    ICacheStorage<String, String> lru2 = new LruMemCacheStorage.Builder<String, String>().enableShareStorage(true).build();
    private final DataManager<String, String> dataManager2 = DataManager.Builder
            .get("简单测试2", source)
            .withCache(lru2)
            .logger(new ConsoleLogger<>())
            .storageId("sameId")
            .build();

    @Test
    public void test() {
        // 访问1的数据源
        DataPack<String> pack1 = dataManager1.getDataPack(null);
        assert pack1.provider == source;
        // 访问1的缓存
        DataPack<String> pack2 = dataManager1.getDataPack(null);
        assert pack2.provider == lru1;
        // 访问2的缓存
        DataPack<String> pack3 = dataManager2.getDataPack(null);
        assert pack3.provider == lru2;
    }

}
