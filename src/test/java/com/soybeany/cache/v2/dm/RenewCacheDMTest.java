package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.component.DBSimulationStorage;
import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.contract.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

/**
 * @author Soybeany
 * @date 2022/2/8
 */
public class RenewCacheDMTest {

    private final IDatasource<String, String> datasource = new IDatasource<String, String>() {
        private boolean canSuccess = true;

        @Override
        public String onGetData(String s) throws Exception {
            if (canSuccess) {
                canSuccess = false;
                return "success";
            }
            throw new Exception("数据源异常");
        }
    };

    ICacheStorage<String, String> lruStorage = new LruMemCacheStorage.Builder<String, String>().capacity(3).pTtl(200).build();
    ICacheStorage<String, String> dbStorage = new DBSimulationStorage<>(200);

    private final DataManager<String, String> dataManager = DataManager.Builder
            .get("简单测试", datasource)
            .withCache(lruStorage)
            .withCache(dbStorage)
            .logger(new ConsoleLogger<>())
            .enableRenewExpiredCache(true)
            .build();

    @Test
    public void test() throws Exception {
        // 第一次将访问数据源
        DataPack<String> dataPack = dataManager.getDataPack(null);
        assert dataPack.provider == datasource;
        // 第二次将访问LRU
        DataPack<String> dataPack2 = dataManager.getDataPack(null);
        assert dataPack2.provider == lruStorage;
        // 第三次将访问续期后的db
        Thread.sleep(200);
        DataPack<String> dataPack3 = dataManager.getDataPack(null);
        assert dataPack3.provider == dbStorage;
        // 第四次将访问LRU
        DataPack<String> dataPack4 = dataManager.getDataPack(null);
        assert dataPack4.provider == lruStorage;
        // 第五次将访问数据源
        dbStorage.enableRenewExpiredCache(false);
        Thread.sleep(250);
        DataPack<String> dataPack5 = dataManager.getDataPack(null);
        assert dataPack5.provider == datasource;
    }

}
