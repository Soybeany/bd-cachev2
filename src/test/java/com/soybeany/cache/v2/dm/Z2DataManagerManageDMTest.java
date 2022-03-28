package com.soybeany.cache.v2.dm;

import com.soybeany.cache.v2.core.DataManager;
import org.junit.Test;

/**
 * @author Soybeany
 * @date 2022/2/10
 */
public class Z2DataManagerManageDMTest {

    @Test
    public void test() {
        assert !DataManager.getAllManagers().isEmpty();
    }

}
