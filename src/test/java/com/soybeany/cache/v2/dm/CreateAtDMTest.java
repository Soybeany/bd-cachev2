package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.model.DataCore;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试缓存数据的创建时间戳(pCreateAt)语义
 */
public class CreateAtDMTest {

    private final AtomicInteger accessCount = new AtomicInteger();

    private final IDatasource<String, String> datasource = s -> {
        accessCount.incrementAndGet();
        return "data_" + s;
    };

    @Test
    public void 首次写入缓存时补齐创建时间且读取保持不变() {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .pTtl(60_000).build();
        DataManager<String, String> dataManager = DataManager.Builder
                .get("createAt首次写入", datasource)
                .withCache(storage)
                .build();
        long before = System.currentTimeMillis();
        DataPack<String> first = dataManager.getDataPack("key");
        long after = System.currentTimeMillis();
        // 首次写入：创建时间非0且在写入时间范围内
        assert first.pCreateAt > 0;
        assert first.pCreateAt >= before && first.pCreateAt <= after : "创建时间应在写入时间范围内";
        // 缓存命中：创建时间保持不变
        DataPack<String> second = dataManager.getDataPack("key");
        assert storage.equals(second.provider);
        assert second.pCreateAt == first.pCreateAt : "缓存命中时创建时间应保持不变";
        assert 1 == accessCount.get();
    }

    @Test
    public void 多级缓存提升时保留原始创建时间() throws Exception {
        ICacheStorage<String, String> l1 = new LruMemCacheStorage.Builder<String, String>().pTtl(200).build();
        ICacheStorage<String, String> l2 = new LruMemCacheStorage.Builder<String, String>().pTtl(60_000).build();
        DataManager<String, String> manager = DataManager.Builder
                .get("createAt提升", datasource)
                .withCache(l1)
                .withCache(l2)
                .build();
        String key = "promo";
        long createAt = manager.getDataPack(key).pCreateAt;
        assert createAt > 0;
        // 等待l1过期，从l2命中并提升至l1
        Thread.sleep(250);
        DataPack<String> pack = manager.getDataPack(key);
        assert l2.equals(pack.provider);
        assert pack.pCreateAt == createAt : "从下一级命中时创建时间应保持不变";
        // 提升后从l1命中，创建时间仍保持不变
        DataPack<String> pack2 = manager.getDataPack(key);
        assert l1.equals(pack2.provider);
        assert pack2.pCreateAt == createAt : "数据提升后创建时间应保持不变";
    }

    @Test
    public void 续期旧数据时保留原始创建时间() throws Exception {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .pTtl(200).build();
        DataManager<String, String> manager = DataManager.Builder
                .get("createAt续期", datasource)
                .withCache(storage)
                .enableRenewExpiredCache(true)
                .build();
        long createAt = manager.getDataPack("key").pCreateAt;
        assert createAt > 0;
        // 等待缓存过期后用null数据源回源，触发续期
        Thread.sleep(250);
        DataPack<String> pack = manager.getDataPack("key", null);
        assert pack.norm() : "续期后应返回旧数据";
        assert pack.pCreateAt == createAt : "续期应保留原始创建时间";
        // 续期数据写回缓存后再次读取，创建时间仍不变
        DataPack<String> pack2 = manager.getDataPack("key", null);
        assert storage.equals(pack2.provider);
        assert pack2.pCreateAt == createAt : "续期写回后创建时间应保持不变";
    }

    @Test
    public void 无创建时间的旧调用方数据包落缓存时自动补齐() {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .pTtl(60_000).build();
        AtomicInteger invokeCount = new AtomicInteger();
        DataManager<String, String> dataManager = DataManager.Builder
                .get("createAt补齐", datasource)
                .withCache(storage)
                .cacheMissHandler((param, cachedPack, fetcher) -> {
                    invokeCount.incrementAndGet();
                    // 模拟旧调用方使用3参构造器，未提供创建时间
                    return new DataPack<>(DataCore.fromData("兜底数据"), "处理器", Long.MAX_VALUE);
                })
                .build();
        DataPack<String> pack = dataManager.getDataPack("key");
        assert "兜底数据".equals(pack.getData());
        // 创建时间未知的数据包，落缓存时以首次写入时间补齐
        assert pack.pCreateAt > 0 : "创建时间未知的包落缓存后应补齐创建时间";
        // 再次命中缓存，创建时间保持为首次补齐的值
        DataPack<String> pack2 = dataManager.getDataPack("key");
        assert storage.equals(pack2.provider);
        assert pack2.pCreateAt == pack.pCreateAt;
        assert 1 == invokeCount.get();
    }

    @Test
    public void 回源时可基于取数结果指定数据的创建时间() {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .pTtl(60_000).build();
        // 模拟数据源侧数据的真实创建时间(早于本次回源时刻)
        long customCreateAt = System.currentTimeMillis() - 10_000L;
        DataManager<String, String> dataManager = DataManager.Builder
                .get("createAt回源指定", datasource)
                .withCache(storage)
                // 自行组装数据包：取数正常时指定创建时间，异常时视为未知
                .cacheMissHandler((param, cachedPack, fetcher) -> {
                    DataCore<String> dataCore = fetcher.getData();
                    return new DataPack<>(dataCore, fetcher.getProvider(), Long.MAX_VALUE, dataCore.norm ? customCreateAt : 0);
                })
                .build();
        // 指定的创建时间直接反映在返回的数据包上，且数据来源为透传的数据源
        DataPack<String> pack = dataManager.getDataPack("key");
        assert "data_key".equals(pack.getData());
        assert datasource.equals(pack.provider) : "应透传fetcher.getProvider()";
        assert pack.pCreateAt == customCreateAt : "应使用调用方指定的创建时间";
        // 落缓存后仍保留指定的创建时间，不会被补齐为写入时间
        DataPack<String> pack2 = dataManager.getDataPack("key");
        assert storage.equals(pack2.provider);
        assert pack2.pCreateAt == customCreateAt : "缓存命中时应保留指定的创建时间";
        assert 1 == accessCount.get();
    }

    @Test
    public void 回源组装时未指定创建时间则按写入时间补齐() {
        ICacheStorage<String, String> storage = new LruMemCacheStorage.Builder<String, String>()
                .pTtl(60_000).build();
        DataManager<String, String> dataManager = DataManager.Builder
                .get("createAt未指定", datasource)
                .withCache(storage)
                .cacheMissHandler((param, cachedPack, fetcher) -> new DataPack<>(fetcher.getData(), fetcher.getProvider(), Long.MAX_VALUE))
                .build();
        long before = System.currentTimeMillis();
        DataPack<String> pack = dataManager.getDataPack("key");
        // 未指定(0=未知)时，落缓存按首次写入时间补齐
        assert pack.pCreateAt >= before && pack.pCreateAt <= System.currentTimeMillis() : "应按写入时间补齐";
    }

}