package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.component.DBSimulationStorage;
import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

/**
 * 测试续期能力({@link DataManager.Builder#enableRenewExpiredCache})
 * <br>回源结果为异常且存在旧的正常数据时，临时激活旧数据，有效期由各级缓存的正常数据有效期决定
 */
public class RenewCacheDMTest {

    private final IDatasource<String, String> datasource = s -> "success";

    private DataManager<String, String> buildManager(boolean enableRenew) {
        ICacheStorage<String, String> lruStorage = new LruMemCacheStorage.Builder<String, String>().pTtl(200).build();
        ICacheStorage<String, String> dbStorage = new DBSimulationStorage<>(200);
        return DataManager.Builder
                .get("续期测试", datasource)
                .withCache(lruStorage)
                .withCache(dbStorage)
                .enableRenewExpiredCache(enableRenew)
                .build();
    }

    @Test
    public void 回源异常时续期旧数据() throws Exception {
        DataManager<String, String> dataManager = buildManager(true);
        // 第一次将访问数据源
        DataPack<String> dataPack = dataManager.getDataPack("key");
        assert dataPack.provider == dataManager.defaultDatasource();
        // 第二次将访问LRU
        dataPack = dataManager.getDataPack("key");
        assert dataPack.provider == dataManager.storages().get(0);
        // 全部过期后，使用null数据源回源(必返异常)，应续期旧的正常数据
        Thread.sleep(250);
        dataPack = dataManager.getDataPack("key", null);
        assert dataPack.norm();
        assert "success".equals(dataPack.getData());
        assert dataPack.provider != dataManager.defaultDatasource();
        // 续期数据已写回一级缓存
        dataPack = dataManager.getDataPack("key");
        assert dataPack.provider == dataManager.storages().get(0);
    }

    @Test
    public void 未开启续期时回源异常按常规缓存() throws Exception {
        DataManager<String, String> dataManager = buildManager(false);
        // 第一次将访问数据源
        DataPack<String> dataPack = dataManager.getDataPack("key");
        assert dataPack.provider == dataManager.defaultDatasource();
        // 全部过期后，使用null数据源回源，返回异常包
        Thread.sleep(250);
        dataPack = dataManager.getDataPack("key", null);
        assert !dataPack.norm();
        // 异常被缓存(防穿透)，再次访问直接命中缓存中的异常
        dataPack = dataManager.getDataPack("key", null);
        assert !dataPack.norm();
        assert dataPack.provider == dataManager.storages().get(0);
    }

}
