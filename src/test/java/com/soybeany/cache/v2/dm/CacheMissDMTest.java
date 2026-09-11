package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.model.DataCore;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试缓存未命中处理器({@link DataManager.Builder#cacheMissHandler})
 */
public class CacheMissDMTest {

    private final AtomicInteger accessCount = new AtomicInteger();

    private final IDatasource<String, String> datasource = s -> {
        accessCount.incrementAndGet();
        return "data_" + s;
    };

    @Test
    public void 未命中时回调处理器并传入旧数据信息() throws Exception {
        List<DataPack<String>> cachedPackHolder = new ArrayList<>();
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .pTtl(300).build();
        DataManager<String, String> dataManager = DataManager.Builder
                .get("未命中回调", datasource)
                .withCache(storage)
                .cacheMissHandler((param, cachedPack, fetcher) -> {
                    cachedPackHolder.add(cachedPack);
                    return fetcher.getData();
                })
                .build();
        // 首次访问：未命中但无旧数据
        dataManager.getData("key");
        assert 1 == accessCount.get();
        assert 1 == cachedPackHolder.size();
        assert null == cachedPackHolder.get(0);
        // 缓存过期后再次访问：未命中且存在过期的缓存数据
        Thread.sleep(400);
        dataManager.getData("key");
        assert 2 == accessCount.get();
        assert 2 == cachedPackHolder.size();
        DataPack<String> cachedPack = cachedPackHolder.get(1);
        assert null != cachedPack;
        assert cachedPack.dataCore.norm;
        assert "data_key".equals(cachedPack.getData());
        // 该场景下收集到的缓存包已过期，pTtl为负值，绝对值即已超时的时长
        assert cachedPack.pTtl < 0;
    }

    @Test
    public void 处理器可返回自定义数据且不访问数据源() {
        AtomicInteger invokeCount = new AtomicInteger();
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .pTtl(60_000).build();
        DataManager<String, String> dataManager = DataManager.Builder
                .get("自定义回源", datasource)
                .withCache(storage)
                .cacheMissHandler((param, cachedPack, fetcher) -> {
                    invokeCount.incrementAndGet();
                    return new DataPack<>(DataCore.fromData("兜底数据"), "处理器", Long.MAX_VALUE);
                })
                .build();
        // 未命中时返回自定义数据，不访问数据源
        assert "兜底数据".equals(dataManager.getData("key"));
        assert 0 == accessCount.get();
        assert 1 == invokeCount.get();
        // 自定义数据已写入缓存，再次访问直接命中，不再回调处理器
        assert "兜底数据".equals(dataManager.getData("key"));
        assert 0 == accessCount.get();
        assert 1 == invokeCount.get();
    }

    @Test
    public void 处理器可基于旧数据返回兜底结果() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .pTtl(300).build();
        DataManager<String, String> dataManager = DataManager.Builder
                .get("旧数据兜底", datasource)
                .withCache(storage)
                .cacheMissHandler((param, cachedPack, fetcher) -> {
                    // 有可用的旧数据时直接返回，否则访问数据源
                    if (null != cachedPack && cachedPack.dataCore.norm) {
                        return new DataPack<>(cachedPack.dataCore, "处理器", Long.MAX_VALUE);
                    }
                    return fetcher.getData();
                })
                .build();
        // 首次访问数据源
        assert "data_key".equals(dataManager.getData("key"));
        assert 1 == accessCount.get();
        // 缓存过期后，处理器基于旧数据兜底，不访问数据源
        Thread.sleep(400);
        assert "data_key".equals(dataManager.getData("key"));
        assert 1 == accessCount.get();
    }

    @Test
    public void 无数据源时处理器收到异常数据包() {
        AtomicInteger invokeCount = new AtomicInteger();
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .pTtl(60_000).build();
        DataManager<String, String> dataManager = DataManager.Builder
                .get("无数据源回源", (IDatasource<String, String>) null)
                .withCache(storage)
                .cacheMissHandler((param, cachedPack, fetcher) -> {
                    invokeCount.incrementAndGet();
                    return fetcher.getData();
                })
                .build();
        // 无数据源时，处理器仍会被调用，fetcher返回包含NoDataSourceException的数据包
        assert !dataManager.containCache("nope");
        assert 1 == invokeCount.get();
    }

}
