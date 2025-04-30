package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.model.DataCore;
import org.junit.Test;

public class SerializeDMTest {

    @Test
    public void test() throws Exception {
        int value = 5;
        DataCore<TestClass> dataCore = DataCore.fromData(new TestClass(value));
        // 简单
        String json = DataCore.toJson(dataCore);
        DataCore<TestClass> result1 = DataCore.fromJson(json, TestClass.class);
        assert result1.data.value == value;
        // 可拓展
        DataCore.JsonInfo jsonInfo = DataCore.toJsonInfo(dataCore);
        DataCore<TestClass> result2 = DataCore.fromJsonInfo(jsonInfo, TestClass.class);
        assert result2.data.value == value;
    }

    private static class TestClass {
        public int value;

        public TestClass(int value) {
            this.value = value;
        }
    }

}
