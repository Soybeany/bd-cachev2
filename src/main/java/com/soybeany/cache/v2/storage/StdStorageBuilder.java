package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.model.DataCore;

import java.util.function.BiFunction;

/**
 * @author Soybeany
 * @date 2022/2/8
 */
public abstract class StdStorageBuilder<Param, Data> {

    /**
     * 正常数据的生存时间，用于一般场景(单位：秒)，与{@link #pTtl}同时设置时，以{@link #pTtl}为准
     */
    private int ttl = Integer.MAX_VALUE - 1;

    /**
     * 异常的生存时间，用于防缓存穿透等场景(单位：秒)，与{@link #pTtlErr}同时设置时，以{@link #pTtlErr}为准
     */
    private int ttlErr = 60;

    /**
     * 正常数据的生存时间，用于一般场景(单位：毫秒)，与{@link #ttl}同时设置时，以此为准
     */
    protected long pTtl;

    /**
     * 异常的生存时间，用于防缓存穿透等场景(单位：毫秒)，与{@link #ttlErr}同时设置时，以此为准
     */
    protected long pTtlErr;

    /**
     * 有效期函数，入参为缓存的数据核心，返回该数据/异常在该级缓存中的有效期(单位：毫秒)
     * <br>* 设置后，{@link #ttl}/{@link #ttlErr}/{@link #pTtl}/{@link #pTtlErr}不再生效
     * <br>* 正常与异常的区分需自行通过{@link DataCore#norm}判断
     */
    private BiFunction<Param, DataCore<Data>, Long> ttlFunction;

    public ICacheStorage<Param, Data> build() {
        // 预处理时间
        handleTtl();
        // 解析有效期函数
        resolveTtlFunction();
        // 构建
        return onBuild();
    }

    public StdStorageBuilder<Param, Data> ttl(int ttl) {
        this.ttl = ttl;
        return this;
    }

    public StdStorageBuilder<Param, Data> ttlErr(int ttlErr) {
        this.ttlErr = ttlErr;
        return this;
    }

    public StdStorageBuilder<Param, Data> pTtl(long pTtl) {
        this.pTtl = pTtl;
        return this;
    }

    public StdStorageBuilder<Param, Data> pTtlErr(long pTtlErr) {
        this.pTtlErr = pTtlErr;
        return this;
    }

    /**
     * 设置有效期函数，入参为缓存的数据核心，返回该数据/异常在该级缓存中的有效期(单位：毫秒)
     * <br>* 设置后，{@link #ttl}/{@link #ttlErr}/{@link #pTtl}/{@link #pTtlErr}不再生效
     * <br>* 正常与异常的区分需自行通过{@link DataCore#norm}判断
     *
     * @param ttlFunction 返回null或负数视为无效配置，会抛出{@link com.soybeany.cache.v2.exception.BdCacheException}
     */
    public StdStorageBuilder<Param, Data> ttl(BiFunction<Param, DataCore<Data>, Long> ttlFunction) {
        this.ttlFunction = ttlFunction;
        return this;
    }

    // ***********************子类重新****************************

    /**
     * 获得解析后的有效期函数，子类构建存储器时使用
     */
    protected BiFunction<Param, DataCore<Data>, Long> ttlFunction() {
        return ttlFunction;
    }

    protected abstract ICacheStorage<Param, Data> onBuild();

    // ***********************内部方法****************************

    private void resolveTtlFunction() {
        // 未配置有效期函数时，使用固定有效期生成默认实现
        if (null == ttlFunction) {
            ttlFunction = (param, dataCore) -> dataCore.norm ? pTtl : pTtlErr;
        }
    }

    private void handleTtl() {
        // 整合
        if (pTtl == 0) {
            pTtl = ttl * 1000L;
        }
        if (pTtlErr == 0) {
            pTtlErr = ttlErr * 1000L;
        }
        // 微调
        pTtl = Math.max(pTtl, 1);
        pTtlErr = Math.max(pTtlErr, 1);
        if (pTtlErr > pTtl) {
            pTtlErr = pTtl;
        }
    }

}
