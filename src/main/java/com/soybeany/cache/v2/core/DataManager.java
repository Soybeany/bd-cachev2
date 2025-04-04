package com.soybeany.cache.v2.core;


import com.soybeany.cache.v2.contract.*;
import com.soybeany.cache.v2.exception.BdCacheException;
import com.soybeany.cache.v2.exception.NoDataSourceException;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataCore;
import com.soybeany.cache.v2.model.DataPack;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * 数据管理器，提供数据自动缓存/读取的核心功能
 *
 * @author Soybeany
 * @date 2020/1/19
 */
@SuppressWarnings({"UnusedReturnValue", "unused"})
public class DataManager<Param, Data> {

    private final DataContext.Core<Param, Data> contextCore;
    private final IDatasource<Param, Data> defaultDatasource;
    private final IKeyConverter<Param> paramDescConverter;
    private final IKeyConverter<Param> paramKeyConverter;
    private final StorageManager<Param, Data> storageManager;

    // ***********************管理****************************

    private DataManager(DataContext.Core<Param, Data> contextCore,
                        IDatasource<Param, Data> defaultDatasource,
                        IKeyConverter<Param> paramDescConverter,
                        IKeyConverter<Param> paramKeyConverter,
                        StorageManager<Param, Data> storageManager) {
        this.contextCore = contextCore;
        this.defaultDatasource = defaultDatasource;
        this.paramDescConverter = paramDescConverter;
        this.paramKeyConverter = paramKeyConverter;
        this.storageManager = storageManager;
    }

    public DataContext.Core<Param, Data> contextCore() {
        return contextCore;
    }

    public IDatasource<Param, Data> defaultDatasource() {
        return defaultDatasource;
    }

    public IKeyConverter<Param> paramDescConverter() {
        return paramDescConverter;
    }

    public IKeyConverter<Param> paramKeyConverter() {
        return paramKeyConverter;
    }

    public List<ICacheStorage<Param, Data>> storages() {
        return storageManager.storages();
    }

    public boolean enableRenewExpiredCache() {
        return storageManager.enableRenewExpiredCache();
    }

    // ********************操作********************

    /**
     * 获得数据(默认方式)
     */
    public Data getData(Param param) {
        return getDataPack(param).getData();
    }

    /**
     * 获得数据(数据包方式)
     */
    public DataPack<Data> getDataPack(Param param) {
        return getDataPack(param, defaultDatasource);
    }

    /**
     * 获得数据(数据包方式)
     */
    public DataPack<Data> getDataPack(Param param, IDatasource<Param, Data> datasource) {
        return getDataPack(param, datasource, true);
    }

    /**
     * 获得数据(数据包方式)
     */
    public DataPack<Data> getDataPack(Param param, IDatasource<Param, Data> datasource, boolean needStore) {
        DataContext<Param> context = getNewDataContext(param);
        DataPack<Data> pack = storageManager.getDataPack(context, datasource, needStore);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onGetData(context, pack, needStore);
        }
        return pack;
    }

    /**
     * 直接从数据源获得数据(不使用缓存)
     */
    public DataPack<Data> getDataPackDirectly(Param param) {
        DataPack<Data> pack = StorageManager.getDataDirectly(this, param, defaultDatasource);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onGetData(getNewDataContext(param), pack, false);
        }
        return pack;
    }

    /**
     * 缓存数据，手动模式管理
     */
    public void cacheData(Param param, Data data) {
        innerCacheData(param, DataCore.fromData(data));
    }

    /**
     * 缓存异常，手动模式管理
     */
    public void cacheException(Param param, RuntimeException e) {
        innerCacheData(param, DataCore.fromException(e));
    }

    /**
     * 批量缓存数据，手动模式管理
     */
    public void batchCacheData(Map<Param, Data> data) {
        batchCache(data, null);
    }

    /**
     * 批量缓存数据/异常，手动模式管理
     */
    public void batchCache(Map<Param, Data> data, Map<Param, RuntimeException> exceptions) {
        Map<Param, DataCore<Data>> dataCores = new HashMap<>();
        if (null != data) {
            data.forEach((k, v) -> dataCores.put(k, DataCore.fromData(v)));
        }
        if (null != exceptions) {
            exceptions.forEach((k, v) -> dataCores.put(k, DataCore.fromException(v)));
        }
        innerBatchCacheData(dataCores);
    }

    /**
     * 失效指定存储器中指定key的缓存
     */
    public void invalidCache(Param param, int... storageIndexes) {
        DataContext<Param> context = getNewDataContext(param);
        storageManager.invalidCache(context, storageIndexes);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onInvalidCache(context, storageIndexes);
        }
    }

    /**
     * 失效指定存储器中全部缓存
     */
    public void invalidAllCache(int... storageIndexes) {
        storageManager.invalidAllCache(contextCore, storageIndexes);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onInvalidAllCache(contextCore, storageIndexes);
        }
    }

    /**
     * 移除指定存储器中指定key的缓存
     */
    public void removeCache(Param param, int... storageIndexes) {
        DataContext<Param> context = getNewDataContext(param);
        storageManager.removeCache(context, storageIndexes);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onRemoveCache(context, storageIndexes);
        }
    }

    /**
     * 清除指定存储器中全部的缓存
     */
    public void clearCache(int... storageIndexes) {
        storageManager.clearCache(contextCore, storageIndexes);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onClearCache(contextCore, storageIndexes);
        }
    }

    /**
     * 指定的缓存是否存在
     */
    public boolean containCache(Param param) {
        DataContext<Param> context = getNewDataContext(param);
        boolean exist = true;
        try {
            storageManager.getDataPack(context, null, false).getData();
        } catch (NoDataSourceException e) {
            exist = false;
        }
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onContainCache(context, exist);
        }
        return exist;
    }

    /**
     * 立刻进行缓存检测
     *
     * @return 缓存是否需要更新
     */
    public boolean checkCache(Param param, ICacheChecker<Param, Data> checker) {
        DataContext<Param> context = getNewDataContext(param);
        return storageManager.checkCache(context, checker);
    }

    // ********************内部方法********************

    private DataContext<Param> getNewDataContext(Param param) {
        return new DataContext<>(contextCore, getNewDataContextParam(param));
    }

    private DataContext.Param<Param> getNewDataContextParam(Param param) {
        String paramKey = paramKeyConverter.getKey(param);
        String paramDesc = paramKey;
        if (null != paramDescConverter && paramDescConverter != paramKeyConverter) {
            paramDesc = paramDescConverter.getKey(param);
        }
        return new DataContext.Param<>(paramDesc, paramKey, param);
    }

    private void innerCacheData(Param param, DataCore<Data> dataCore) {
        DataContext<Param> context = getNewDataContext(param);
        DataPack<Data> pack = new DataPack<>(dataCore, this, Long.MAX_VALUE);
        storageManager.cacheData(context, pack);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onCacheData(context, pack);
        }
    }

    private void innerBatchCacheData(Map<Param, DataCore<Data>> dataCores) {
        Map<DataContext.Param<Param>, DataPack<Data>> dataPacks = new HashMap<>();
        dataCores.forEach((param, dataCore) ->
                dataPacks.put(getNewDataContextParam(param), new DataPack<>(dataCore, this, Long.MAX_VALUE))
        );
        storageManager.batchCacheData(contextCore, dataPacks);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onBatchCacheData(contextCore, dataPacks);
        }
    }

    // ********************内部类********************

    public static class Builder<Param, Data> {

        private final StorageManager<Param, Data> storageManager = new StorageManager<>();
        private final String dataDesc;
        private final IDatasource<Param, Data> defaultDatasource;
        private final IKeyConverter<Param> paramKeyConverter;

        private String storageId;

        private IKeyConverter<Param> paramDescConverter;

        private ILogger<Param, Data> logger;

        public static <Data> Builder<String, Data> get(String dataDesc, IDatasource<String, Data> datasource) {
            return new Builder<>(dataDesc, datasource, new IKeyConverter.Std());
        }

        public static <Param, Data> Builder<Param, Data> get(String dataDesc, IDatasource<Param, Data> datasource, IKeyConverter<Param> keyConverter) {
            return new Builder<>(dataDesc, datasource, keyConverter);
        }

        private Builder(String dataDesc, IDatasource<Param, Data> datasource, IKeyConverter<Param> keyConverter) {
            this.dataDesc = Optional.ofNullable(dataDesc).orElseThrow(() -> new BdCacheException("dataDesc不能为null"));
            Optional.ofNullable(keyConverter).orElseThrow(() -> new BdCacheException("keyConverter不能为null"));
            this.defaultDatasource = datasource;
            this.paramKeyConverter = keyConverter;
            this.paramDescConverter = keyConverter;
        }

        /**
         * 数据存储的唯一id，某些存储方式会将相同的storageId共享存储
         */
        public Builder<Param, Data> storageId(String storageId) {
            this.storageId = storageId;
            return this;
        }

        /**
         * 配置入参描述转换器
         * <br>* 根据入参定义自动输出的日志中使用的paramDesc
         * <br>* 默认使用构造时指定的“defaultConverter”
         */
        public Builder<Param, Data> paramDescConverter(IKeyConverter<Param> paramDescConverter) {
            this.paramDescConverter = paramDescConverter;
            return this;
        }

        /**
         * 若需要记录日志，则配置该logger
         */
        public Builder<Param, Data> logger(ILogger<Param, Data> logger) {
            this.logger = logger;
            return this;
        }

        /**
         * 是否允许在数据源出现异常时，临时激活上一次已失效的缓存数据，使用异常时的生存时间
         */
        public Builder<Param, Data> enableRenewExpiredCache(boolean flag) {
            storageManager.enableRenewExpiredCache(flag);
            return this;
        }

        /**
         * 启用数据检查
         *
         * @param intervalSupplier 检查间隔提供者(单位：毫秒)
         */
        public Builder<Param, Data> enableDataCheck(Function<Param, Long> intervalSupplier, ICacheChecker<Param, Data> checker) {
            storageManager.setDataChecker(intervalSupplier, checker);
            return this;
        }

        // ********************设置********************

        /**
         * 使用缓存存储器，可以多次调用，形成多级缓存
         * <br>第一次调用为一级缓存，第二次为二级缓存...以此类推
         * <br>数据查找时一级缓存最先被触发
         */
        public Builder<Param, Data> withCache(ICacheStorage<Param, Data> storage) {
            if (null == storage) {
                throw new BdCacheException("storage不能为null");
            }
            // 添加到存储器列表
            storageManager.addStorage(storage);
            return this;
        }

        /**
         * 设置等待锁的时间
         */
        public Builder<Param, Data> lockWaitTime(long seconds) {
            storageManager.lockWaitTime(seconds);
            return this;
        }

        /**
         * 新增缓存失效的监听器（单条，非全部）
         */
        public Builder<Param, Data> onInvalidListener(IOnInvalidListener<Param> listener) {
            storageManager.addOnInvalidListener(listener);
            return this;
        }

        /**
         * 构建出用于使用的实例
         */
        public DataManager<Param, Data> build() {
            DataContext.Core<Param, Data> core = new DataContext.Core<>(dataDesc, Optional.ofNullable(this.storageId).orElse(dataDesc), logger);
            storageManager.init(core);
            // 创建管理器实例
            return new DataManager<>(core, defaultDatasource, paramDescConverter, paramKeyConverter, storageManager);
        }
    }
}
