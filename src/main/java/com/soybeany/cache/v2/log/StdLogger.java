package com.soybeany.cache.v2.log;

import com.soybeany.cache.v2.contract.frame.ICacheStorage;
import com.soybeany.cache.v2.contract.frame.ILogger;
import com.soybeany.cache.v2.contract.user.IDatasource;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataPack;
import com.soybeany.cache.v2.model.DataParam;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Soybeany
 * @date 2020/12/8
 */
public class StdLogger implements ILogger {

    private final ILogWriter mWriter;
    private DataContext context;

    public StdLogger(ILogWriter writer) {
        mWriter = writer;
    }

    @Override
    public void onInit(DataContext context) {
        this.context = context;
    }

    @Override
    public <Param, Data> void onGetData(DataParam<Param> param, DataPack<Data> pack, boolean needStore) {
        String from = getFrom(pack.provider, needStore);
        String dataDesc = getDataDesc();
        String paramDesc = getParamDesc(param);
        if (pack.norm()) {
            mWriter.onWriteInfo("“" + dataDesc + "”从“" + from + "”获取了“" + paramDesc + "”的数据");
        } else {
            mWriter.onWriteInfo("“" + dataDesc + "”从“" + from + "”获取了“" + paramDesc + "”的异常(" + getExceptionMsg(pack) + ")");
        }
    }

    @Override
    public <Param, Data> void onCacheData(DataParam<Param> param, DataPack<Data> pack) {
        String dataDesc = getDataDesc();
        String paramDesc = getParamDesc(param);
        if (pack.norm()) {
            mWriter.onWriteInfo("“" + dataDesc + "”缓存了“" + paramDesc + "”的数据");
        } else {
            mWriter.onWriteWarn("“" + dataDesc + "”缓存了“" + paramDesc + "”的异常(" + getExceptionMsg(pack) + ")");
        }
    }

    @Override
    public <Param, Data> void onBatchCacheData(Map<DataParam<Param>, DataPack<Data>> dataPacks) {
        String dataDesc = getDataDesc();
        List<String> dataList = new ArrayList<>();
        List<String> exceptionList = new ArrayList<>();
        // 分类存储
        dataPacks.forEach((param, pack) -> {
            String paramDesc = getParamDesc(param);
            List<String> list = pack.norm() ? dataList : exceptionList;
            list.add(paramDesc);
        });
        // 打印
        if (!dataList.isEmpty()) {
            mWriter.onWriteInfo("“" + dataDesc + "”批量缓存了数据，“" + dataList + "”");
        }
        if (!exceptionList.isEmpty()) {
            mWriter.onWriteWarn("“" + dataDesc + "”批量缓存了异常，“" + exceptionList + "”");
        }
    }

    @Override
    public <Param> void onInvalidCache(DataParam<Param> param, int... storageIndexes) {
        mWriter.onWriteInfo("“" + getDataDesc() + "”失效了" + getIndexMsg(storageIndexes) + "中“" + getParamDesc(param) + "”的缓存");
    }

    @Override
    public void onInvalidAllCache(int... storageIndexes) {
        mWriter.onWriteInfo("“" + context.dataDesc + "”失效了" + getIndexMsg(storageIndexes) + "的缓存");
    }

    @Override
    public <Param> void onRemoveCache(DataParam<Param> param, int... storageIndexes) {
        mWriter.onWriteInfo("“" + getDataDesc() + "”移除了" + getIndexMsg(storageIndexes) + "中“" + getParamDesc(param) + "”的缓存");
    }

    @Override
    public <Param> void onRenewExpiredCache(DataParam<Param> param, Object provider) {
        mWriter.onWriteInfo("“" + getDataDesc() + "”在“" + getFrom(provider, true) + "”续期了“" + getParamDesc(param) + "”的缓存");
    }

    @Override
    public void onClearCache(int... storageIndexes) {
        mWriter.onWriteInfo("“" + context.dataDesc + "”清空了" + getIndexMsg(storageIndexes) + "的缓存");
    }

    @Override
    public <Param> void onContainCache(DataParam<Param> param, boolean exist) {
        mWriter.onWriteInfo("“" + getDataDesc() + "”" + (exist ? "存在" : "没有") + "“" + getParamDesc(param) + "”的缓存");
    }

    @Override
    public <Param> void onCheckCache(DataParam<Param> param, boolean needUpdate) {
        mWriter.onWriteInfo("“" + getDataDesc() + "”" + (needUpdate ? "需要" : "无需") + "更新“" + getParamDesc(param) + "”的缓存");
    }

    private String getFrom(Object provider, boolean needStore) {
        if (provider instanceof ICacheStorage) {
            return "缓存(" + ((ICacheStorage<?, ?>) provider).desc() + ")";
        } else if (provider instanceof IDatasource) {
            return "数据源(" + (needStore ? "存储" : "只读") + ")";
        }
        return "其它来源(" + provider + ")";
    }

    private String getDataDesc() {
        String desc = context.dataDesc;
        if (!Objects.equals(context.dataDesc, context.storageId)) {
            desc += "(" + context.storageId + ")";
        }
        return desc;
    }

    private <Param> String getParamDesc(DataParam<Param> dataParam) {
        String desc = dataParam.paramDesc;
        if (!Objects.equals(dataParam.paramDesc, dataParam.paramKey)) {
            desc += "(" + dataParam.paramKey + ")";
        }
        return desc;
    }

    private <Data> String getExceptionMsg(DataPack<Data> pack) {
        Exception exception = pack.dataCore.exception;
        return exception.getClass().getSimpleName() + " - " + exception.getMessage();
    }

    private String getIndexMsg(int... storageIndexes) {
        if (null == storageIndexes || 0 == storageIndexes.length) {
            return "全部存储器";
        }
        List<Integer> list = new ArrayList<>();
        for (int index : storageIndexes) {
            list.add(index);
        }
        return "下标为" + list + "的存储器";
    }
}
