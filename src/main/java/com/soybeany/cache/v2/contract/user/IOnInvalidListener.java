package com.soybeany.cache.v2.contract.user;

public interface IOnInvalidListener<Param> {
    void onInvoke(Param param, int... storageIndexes);
}
