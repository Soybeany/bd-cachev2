package com.soybeany.cache.v2.component;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.model.DataParam;

/**
 * 可注入异常的存储器包装类，用于测试返回值的准确性
 */
public class FaultInjectStorage<Param, Data> implements ICacheStorage<Param, Data> {

    private final ICacheStorage<Param, Data> delegate;

    private RuntimeException exOnCacheData;
    private RuntimeException exOnInvalidCache;
    private RuntimeException exOnInvalidAllCache;
    private RuntimeException exOnRemoveCache;

    public FaultInjectStorage(ICacheStorage<Param, Data> delegate) {
        this.delegate = delegate;
    }

    public FaultInjectStorage<Param, Data> exOnCacheData(RuntimeException e) {
        this.exOnCacheData = e;
        return this;
    }

    public FaultInjectStorage<Param, Data> exOnInvalidCache(RuntimeException e) {
        this.exOnInvalidCache = e;
        return this;
    }

    public FaultInjectStorage<Param, Data> exOnInvalidAllCache(RuntimeException e) {
        this.exOnInvalidAllCache = e;
        return this;
    }

    public FaultInjectStorage<Param, Data> exOnRemoveCache(RuntimeException e) {
        this.exOnRemoveCache = e;
        return this;
    }

    @Override
    public String desc() {
        return delegate.desc();
    }

    @Override
    public void onInit(DataContext context) {
        delegate.onInit(context);
    }

    @Override
    public DataPack<Data> onGetCache(DataParam<Param> param) {
        return delegate.onGetCache(param);
    }

    @Override
    public DataPack<Data> onGetCacheIgnoreExpiry(DataParam<Param> param) {
        return delegate.onGetCacheIgnoreExpiry(param);
    }

    @Override
    public DataPack<Data> onCacheData(DataParam<Param> param, DataPack<Data> dataPack) {
        if (null != exOnCacheData) {
            throw exOnCacheData;
        }
        return delegate.onCacheData(param, dataPack);
    }

    @Override
    public void onInvalidCache(DataParam<Param> param) {
        if (null != exOnInvalidCache) {
            throw exOnInvalidCache;
        }
        delegate.onInvalidCache(param);
    }

    @Override
    public void onInvalidAllCache() {
        if (null != exOnInvalidAllCache) {
            throw exOnInvalidAllCache;
        }
        delegate.onInvalidAllCache();
    }

    @Override
    public void onRemoveCache(DataParam<Param> param) {
        if (null != exOnRemoveCache) {
            throw exOnRemoveCache;
        }
        delegate.onRemoveCache(param);
    }

    @Override
    public void onClearCache() {
        delegate.onClearCache();
    }

    @Override
    public void enableRenewExpiredCache(boolean enable) {
        delegate.enableRenewExpiredCache(enable);
    }

    @Override
    public int cachedDataCount() {
        return delegate.cachedDataCount();
    }
}
