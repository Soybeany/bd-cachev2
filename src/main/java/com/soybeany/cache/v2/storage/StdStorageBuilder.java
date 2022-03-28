package com.soybeany.cache.v2.storage;

import com.soybeany.cache.v2.contract.ICacheStorage;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * @author Soybeany
 * @date 2022/2/8
 */
@Accessors(fluent = true, chain = true)
public abstract class StdStorageBuilder<Param, Data> {

    /**
     * 正常数据的生存时间，用于一般场景(单位：秒)，与{@link #pTtl}同时设置时，以{@link #pTtl}为准
     */
    @Setter
    private int ttl = Integer.MAX_VALUE / 1000 - 1;

    /**
     * 异常的生存时间，用于防缓存穿透等场景(单位：秒)，与{@link #pTtlErr}同时设置时，以{@link #pTtlErr}为准
     */
    @Setter
    private int ttlErr = 60;

    /**
     * 正常数据的生存时间，用于一般场景(单位：毫秒)，与{@link #ttl}同时设置时，以此为准
     */
    @Setter
    protected int pTtl;

    /**
     * 异常的生存时间，用于防缓存穿透等场景(单位：毫秒)，与{@link #ttlErr}同时设置时，以此为准
     */
    @Setter
    protected int pTtlErr;

    public ICacheStorage<Param, Data> build() {
        // 预处理时间
        handleTtl();
        // 构建
        return onBuild();
    }

    // ***********************子类重新****************************

    protected abstract ICacheStorage<Param, Data> onBuild();

    // ***********************内部方法****************************

    private void handleTtl() {
        // 整合
        if (pTtl == 0) {
            pTtl = ttl * 1000;
        }
        if (pTtlErr == 0) {
            pTtlErr = ttlErr * 1000;
        }
        // 微调
        pTtl = Math.max(pTtl, 1);
        pTtlErr = Math.max(pTtlErr, 1);
        if (pTtlErr > pTtl) {
            pTtlErr = pTtl;
        }
    }

}
