package com.soybeany.cache.v2.log;

import com.soybeany.cache.v2.contract.ICacheStorage;
import com.soybeany.cache.v2.contract.IDatasource;
import com.soybeany.cache.v2.contract.ILogger;
import com.soybeany.cache.v2.model.DataContext;
import com.soybeany.cache.v2.model.DataPack;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Soybeany
 * @date 2020/12/8
 */
public class StdLogger<Param, Data> implements ILogger<Param, Data> {

    private final ILogWriter mWriter;

    public StdLogger(ILogWriter writer) {
        mWriter = writer;
    }

    @Override
    public void onGetData(DataContext<Param> context, DataPack<Data> pack) {
        String from = getFrom(pack.provider);
        String dataDesc = getDataDesc(context);
        String paramDesc = getParamDesc(context);
        if (pack.norm()) {
            mWriter.onWriteInfo("“" + dataDesc + "”从“" + from + "”获取了“" + paramDesc + "”的数据");
        } else {
            mWriter.onWriteInfo("“" + dataDesc + "”从“" + from + "”获取了“" + paramDesc + "”的异常(" + getExceptionMsg(pack) + ")");
        }
    }

    @Override
    public void onCacheData(DataContext<Param> context, DataPack<Data> pack) {
        String dataDesc = getDataDesc(context);
        String paramDesc = getParamDesc(context);
        if (pack.norm()) {
            mWriter.onWriteInfo("“" + dataDesc + "”缓存了“" + paramDesc + "”的数据");
        } else {
            mWriter.onWriteWarn("“" + dataDesc + "”缓存了“" + paramDesc + "”的异常(" + getExceptionMsg(pack) + ")");
        }
    }

    @Override
    public void onRemoveCache(DataContext<Param> context, int... storageIndexes) {
        mWriter.onWriteInfo("“" + getDataDesc(context) + "”移除了" + getIndexMsg(storageIndexes) + "中“" + getParamDesc(context) + "”的缓存");
    }

    @Override
    public void onRenewExpiredCache(DataContext<Param> context, Object provider) {
        mWriter.onWriteInfo("“" + getDataDesc(context) + "”在“" + getFrom(provider) + "”续期了“" + getParamDesc(context) + "”的缓存");
    }

    @Override
    public void onClearCache(String dataDesc, int... storageIndexes) {
        mWriter.onWriteInfo("“" + dataDesc + "”清空了" + getIndexMsg(storageIndexes) + "的缓存");
    }

    private String getFrom(Object provider) {
        if (provider instanceof ICacheStorage) {
            return "缓存(" + ((ICacheStorage<?, ?>) provider).desc() + ")";
        } else if (provider instanceof IDatasource) {
            return "数据源";
        }
        return "其它来源(" + provider + ")";
    }

    private String getDataDesc(DataContext<Param> context) {
        String desc = context.dataDesc;
        if (!Objects.equals(context.dataDesc, context.storageId)) {
            desc += "(" + context.storageId + ")";
        }
        return desc;
    }

    private String getParamDesc(DataContext<Param> context) {
        String desc = context.paramDesc;
        if (!Objects.equals(context.paramDesc, context.paramKey)) {
            desc += "(" + context.paramKey + ")";
        }
        return desc;
    }

    private String getExceptionMsg(DataPack<Data> pack) {
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
