package com.soybeany.cache.v2.contract.user;

import java.util.List;
import java.util.Map;

public interface IOnInvalidListener<Param> {
    void onInvoke(Param param, List<Integer> onSuccess, Map<Integer, Exception> onException);
}
