package org.pangolin.xuzhe.pipeline;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * Created by ubuntu on 17-6-12.
 */
public class MyStringBuilder {
    byte[] value;
    int count;
    public MyStringBuilder(int size) {
        value = new byte[size];
        count = 0;
    }
    public MyStringBuilder() {
        this(200);
    }
    private void ensureCapacityInternal(int minimumCapacity) {
        if (minimumCapacity - value.length > 0)
            expandCapacity(minimumCapacity);
    }

    void expandCapacity(int minimumCapacity) {
        int newCapacity = value.length * 2 + 2;
        if (newCapacity - minimumCapacity < 0)
            newCapacity = minimumCapacity;
        if (newCapacity < 0) {
            if (minimumCapacity < 0) // overflow
                throw new OutOfMemoryError();
            newCapacity = Integer.MAX_VALUE;
        }
        value = Arrays.copyOf(value, newCapacity);
    }

    public void append(byte b) {
        ensureCapacityInternal(count + 1);
        value[count++] = b;
    }

    public String toString() {
        try {
            return new String(value, 0, count, "utf-8");
        } catch (UnsupportedEncodingException e) {

        }
        return "";
    }

    public void clear() {
        count = 0;
    }

    public int getSize() {
        return count;
    }
}
