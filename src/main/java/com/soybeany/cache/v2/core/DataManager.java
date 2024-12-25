package com.soybeany.cache.v2.core;


import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.contract.IDatasource;
import com.soybeany.cache.v2.contract.IKeyConverter;
import com.soybeany.cache.v2.contract.ILogger;
import com.soybeany.cache.v2.exception.BdCacheException;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataCore;
import com.soybeany.cache.v2.model.DataPack;

import java.util.*;

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
    private final CacheNode<Param, Data> firstNode;

    private final List<ICacheStorage<Param, Data>> storages;
    private final boolean enableRenewExpiredCache;

    // ***********************管理****************************

    private DataManager(DataContext.Core<Param, Data> contextCore,
                        IDatasource<Param, Data> defaultDatasource,
                        IKeyConverter<Param> paramDescConverter,
                        IKeyConverter<Param> paramKeyConverter,
                        CacheNode<Param, Data> firstNode,
                        List<ICacheStorage<Param, Data>> storages,
                        boolean enableRenewExpiredCache) {
        this.contextCore = contextCore;
        this.defaultDatasource = defaultDatasource;
        this.paramDescConverter = paramDescConverter;
        this.paramKeyConverter = paramKeyConverter;
        this.firstNode = firstNode;
        this.storages = storages;
        this.enableRenewExpiredCache = enableRenewExpiredCache;
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
        return storages;
    }

    public boolean enableRenewExpiredCache() {
        return enableRenewExpiredCache;
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
        // 没有缓存节点的情况
        if (null == firstNode) {
            return innerGetDataPackDirectly(param, datasource);
        }
        // 有缓存节点的情况
        DataContext<Param> context = getNewDataContext(param);
        DataPack<Data> pack = firstNode.getDataPackAndAutoCache(context, datasource);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onGetData(context, pack);
        }
        return pack;
    }

    /**
     * 直接从数据源获得数据(不使用缓存)
     */
    public DataPack<Data> getDataPackDirectly(Param param) {
        return innerGetDataPackDirectly(param, defaultDatasource);
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
        if (null == firstNode) {
            return;
        }
        DataContext<Param> context = getNewDataContext(param);
        firstNode.invalidCache(context, storageIndexes);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onInvalidCache(context, storageIndexes);
        }
    }

    /**
     * 失效指定存储器中全部缓存
     */
    public void invalidAllCache(int... storageIndexes) {
        if (null == firstNode) {
            return;
        }
        firstNode.invalidAllCache(contextCore, storageIndexes);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onInvalidAllCache(contextCore, storageIndexes);
        }
    }

    /**
     * 移除指定存储器中指定key的缓存
     */
    public void removeCache(Param param, int... storageIndexes) {
        if (null == firstNode) {
            return;
        }
        DataContext<Param> context = getNewDataContext(param);
        firstNode.removeCache(context, storageIndexes);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onRemoveCache(context, storageIndexes);
        }
    }

    /**
     * 清除指定存储器中全部的缓存
     */
    public void clearCache(int... storageIndexes) {
        if (null == firstNode) {
            return;
        }
        firstNode.clearCache(contextCore, storageIndexes);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onClearCache(contextCore, storageIndexes);
        }
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

    private DataPack<Data> innerGetDataPackDirectly(Param param, IDatasource<Param, Data> datasource) {
        DataPack<Data> pack = CacheNode.getDataDirectly(this, param, datasource);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onGetData(getNewDataContext(param), pack);
        }
        return pack;
    }

    private void innerCacheData(Param param, DataCore<Data> dataCore) {
        if (null == firstNode) {
            return;
        }
        // 缓存数据
        DataContext<Param> context = getNewDataContext(param);
        DataPack<Data> pack = new DataPack<>(dataCore, this, Long.MAX_VALUE);
        // 得到最后一个节点
        CacheNode<Param, Data> lastNode = firstNode;
        while (null != lastNode.getNextNode()) {
            lastNode = lastNode.getNextNode();
        }
        lastNode.cacheData(context, pack);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onCacheData(context, pack);
        }
    }

    private void innerBatchCacheData(Map<Param, DataCore<Data>> dataCores) {
        if (null == firstNode) {
            return;
        }
        // 生成缓存数据
        Map<DataContext.Param<Param>, DataPack<Data>> dataPacks = new HashMap<>();
        dataCores.forEach((param, dataCore) ->
                dataPacks.put(getNewDataContextParam(param), new DataPack<>(dataCore, this, Long.MAX_VALUE))
        );
        // 得到最后一个节点
        CacheNode<Param, Data> lastNode = firstNode;
        while (null != lastNode.getNextNode()) {
            lastNode = lastNode.getNextNode();
        }
        lastNode.batchCacheData(contextCore, dataPacks);
        // 记录日志
        if (null != contextCore.logger) {
            contextCore.logger.onBatchCacheData(contextCore, dataPacks);
        }
    }

    // ********************内部类********************

    public static class Builder<Param, Data> {

        private final List<CacheNode<Param, Data>> mNodes = new ArrayList<>();
        private final String dataDesc;
        private final IDatasource<Param, Data> defaultDatasource;
        private final IKeyConverter<Param> paramKeyConverter;

        /**
         * 数据存储的唯一id，某些存储方式会将相同的storageId共享存储
         */
        private String storageId;

        /**
         * 配置入参描述转换器
         * <br>* 根据入参定义自动输出的日志中使用的paramDesc
         * <br>* 默认使用构造时指定的“defaultConverter”
         */
        private IKeyConverter<Param> paramDescConverter;

        /**
         * 若需要记录日志，则配置该logger
         */
        private ILogger<Param, Data> logger;

        /**
         * 是否允许在数据源出现异常时，使用上一次已失效的缓存数据，使用异常的生存时间
         */
        private boolean enableRenewExpiredCache;

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

        public Builder<Param, Data> storageId(String storageId) {
            this.storageId = storageId;
            return this;
        }

        public Builder<Param, Data> paramDescConverter(IKeyConverter<Param> paramDescConverter) {
            this.paramDescConverter = paramDescConverter;
            return this;
        }

        public Builder<Param, Data> logger(ILogger<Param, Data> logger) {
            this.logger = logger;
            return this;
        }

        public Builder<Param, Data> enableRenewExpiredCache(boolean flag) {
            this.enableRenewExpiredCache = flag;
            return this;
        }

        // ********************设置********************

        /**
         * 使用缓存存储器，可以多次调用，形成多级缓存
         * <br>第一次调用为一级缓存，第二次为二级缓存...以此类推
         * <br>数据查找时一级缓存最先被触发
         * <br/>自定义storage时需注意，多级缓存，最终的优先级会逐级减一
         */
        public Builder<Param, Data> withCache(ICacheStorage<Param, Data> storage) {
            if (null == storage) {
                throw new BdCacheException("storage不能为null");
            }
            // 添加到存储器列表
            mNodes.add(new CacheNode<>(storage, storage.priority() - mNodes.size(), storage.lockWaitTime()));
            return this;
        }

        /**
         * 构建出用于使用的实例
         */
        public DataManager<Param, Data> build() {
            // 初始化存储
            List<ICacheStorage<Param, Data>> storages = new ArrayList<>();
            DataContext.Core<Param, Data> core = initContextCore(storages);
            // 节点排序
            mNodes.sort(new ServiceComparator());
            // 为末端节点的存储设置缓存重用
            if (!mNodes.isEmpty() && enableRenewExpiredCache) {
                mNodes.get(0).getCurStorage().enableRenewExpiredCache(true);
            }
            // 创建调用链
            CacheNode<Param, Data> firstNode = buildChain();
            // 创建管理器实例
            return new DataManager<>(
                    core,
                    defaultDatasource,
                    paramDescConverter,
                    paramKeyConverter,
                    firstNode,
                    Collections.unmodifiableList(storages),
                    enableRenewExpiredCache
            );
        }

        // ********************内部方法********************

        private DataContext.Core<Param, Data> initContextCore(List<ICacheStorage<Param, Data>> storages) {
            String storageId = Optional.ofNullable(this.storageId).orElse(dataDesc);
            DataContext.Core<Param, Data> core = new DataContext.Core<>(dataDesc, storageId, logger);
            for (CacheNode<Param, Data> node : mNodes) {
                ICacheStorage<Param, Data> storage = node.getCurStorage();
                storage.onInit(core);
                storages.add(storage);
            }
            return core;
        }

        private CacheNode<Param, Data> buildChain() {
            CacheNode<Param, Data> nextNode = null;
            for (CacheNode<Param, Data> node : mNodes) {
                node.setNextNode(nextNode);
                nextNode = node;
            }
            return nextNode;
        }

        /**
         * 用于缓存服务的排序器
         */
        private static class ServiceComparator implements Comparator<CacheNode<?, ?>> {
            @Override
            public int compare(CacheNode o1, CacheNode o2) {
                return o1.getPriority() - o2.getPriority();
            }
        }
    }

}
