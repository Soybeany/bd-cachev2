package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.core.DataManager;
import com.soybeany.cache.v2.log.ConsoleLogger;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.model.DataParam;
import com.soybeany.cache.v2.storage.LruMemCacheStorage;
import org.junit.Test;

import java.util.UUID;

/**
 * <br>Created by Soybeany on 2020/11/17.
 */
public class MultiKeyDMTest {

    private final ICacheStorage<String, String> lruStorage = new TestStorage<>(800);

    private final DataManager<String, String> dataManager = DataManager.Builder
            .get("MultiExpiry", s -> {
                System.out.println("“" + s + "”access datasource");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException("休眠中断:" + e.getMessage());
                }
                return s;
            })
            .withCache(lruStorage)
            .logger(new ConsoleLogger())
            .build();

    @Test
    public void test() throws Exception {
        int count = 10;
        Thread[] threads = new Thread[count];
        for (int i = 0; i < count; i++) {
            threads[i] = new Thread(() -> {
                String key = UUID.randomUUID().toString();
                dataManager.getDataPack(key);
            });
            threads[i].start();
        }
        long start = System.currentTimeMillis();
        for (Thread thread : threads) {
            thread.join();
        }
        long delta = System.currentTimeMillis() - start;
        System.out.println("时差:" + delta);
        // 不能高于2秒：访问数据源不允许串行；整体执行效率也不能过低
        assert delta < 2000;
    }

    private static class TestStorage<Param, Data> extends LruMemCacheStorage<Param, Data> {

        public TestStorage(int pTtl) {
            super(pTtl, 60 * 1000, 100);
        }

        @SuppressWarnings("CallToPrintStackTrace")
        @Override
        public DataPack<Data> onCacheData(DataParam<Param> param, DataPack<Data> dataPack) {
            System.out.println("存数据:" + param.paramKey);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return super.onCacheData(param, dataPack);
        }
    }

}
