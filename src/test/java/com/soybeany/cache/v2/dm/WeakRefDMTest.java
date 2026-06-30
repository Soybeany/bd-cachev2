package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

/**
 * @author Soybeany
 * @date 2022/9/13
 */
public class WeakRefDMTest {

    private final IDatasource<String, Data> datasource = key -> new Data(key, 123);

    private final ICacheStorage<String, Data> storage = new LruMemCacheStorage.Builder<String, Data>().weakRef(true).build();

    private final DataManager<String, Data> dataManager = DataManager.Builder
            .get("弱引用测试", datasource)
            .withCache(storage)
            .logger(new ConsoleLogger())
            .build();

    @Test
    public void test_弱引用GC后重新拉取数据() {
        String key = "成功";
        // 默认为同一副本
        DataPack<Data> dataPack1 = dataManager.getDataPack(key);
        DataPack<Data> dataPack2 = dataManager.getDataPack(key);
        assert dataPack1.getData() == dataPack2.getData() : "同一key应返回同一对象引用";
        assert dataPack2.provider == storage : "应从缓存读取";
        // GC后重新拉取数据（多次尝试确保GC生效）
        boolean gcTriggered = false;
        for (int i = 0; i < 5; i++) {
            System.gc();
            DataPack<Data> dataPack3 = dataManager.getDataPack(key);
            if (dataPack3.provider == datasource) {
                gcTriggered = true;
                break;
            }
        }
        assert gcTriggered : "GC后应重新从数据源获取数据";
    }

    private static class Data {
        String key;
        Integer b;

        public Data(String key, Integer b) {
            this.key = key;
            this.b = b;
        }
    }

}
