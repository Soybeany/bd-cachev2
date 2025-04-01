package com.soybeany.cache.v2.contract;

public interface IOnInvalidListener<Param> {
    void onInvoke(Param param, int... storageIndexes);
}
