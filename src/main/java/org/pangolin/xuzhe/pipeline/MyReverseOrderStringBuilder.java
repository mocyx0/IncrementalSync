package org.pangolin.xuzhe.pipeline;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * Created by ubuntu on 17-6-12.
 */
public class MyReverseOrderStringBuilder {
    byte[] value;
    int remain;
    public MyReverseOrderStringBuilder(int size) {
        value = new byte[size];
        remain = value.length;
    }
    public MyReverseOrderStringBuilder() {
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
//        try {
            ensureCapacityInternal((value.length - remain) + 1);
            value[--remain] = b;
//        } catch (ArrayIndexOutOfBoundsException e) {
//            e.printStackTrace();
//        }
    }

    public String toString() {
        try {
            return new String(value, remain, value.length-remain, "utf-8");
        } catch (UnsupportedEncodingException e) {

        }
        return "";
    }

    public void clear() {
        remain = value.length;
    }

    public int getSize() {
        return value.length - remain;
    }
}
