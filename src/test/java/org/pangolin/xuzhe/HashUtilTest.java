package org.pangolin.xuzhe;

import org.junit.Test;
import static org.junit.Assert.*;
/**
 * Created by ubuntu on 17-6-5.
 */
public class HashUtilTest {
    @Test
    public void hashTest() {
        byte[] data = {30, 0, 0, 0, 0, 0, 0, 0, 0, 0,};

        assertEquals(30, HashUtil.hash(data, 1));
        data[1] = 30;
        data[2] = 1;
        assertEquals(29761, HashUtil.hash(data, 3));

    }

}
