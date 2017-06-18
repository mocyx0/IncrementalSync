package org.pangolin.xuzhe.positiveorder;

/**
 * Created by 29146 on 2017/6/16.
 */
public class Record {
    private long pk; //主键值
    private byte[] columnValueArray;  // TODO 暂时使用8字节存储每列的值
    private byte[] columnValueSizeArray;
    public Record(long pk,int columnSize){
        this.pk = pk;
        this.columnValueArray = new byte[8*columnSize];
        this.columnValueSizeArray = new byte[columnSize];
    }

    public void setPk(long pk) {
        this.pk = pk;
    }

    public long getPk() {
        return pk;
    }


//    public ByteBuffer[] getColumnValue(int columnIndex) {
//        return columnValue;
//    }
    public void setColumnValue(int columnIndex, byte[] dataSrc, int offset, int length) {
        if(length > 8) {
            throw new RuntimeException("columnValueLength 大于 8");
        }
        columnValueSizeArray[columnIndex] = (byte)length;
        System.arraycopy(dataSrc, offset, columnValueArray, columnIndex*8, length);

    }

    public String toString() {
        StringBuilder sb = new StringBuilder(100);
        sb.append(pk);
        for(int i = 0; i < columnValueArray.length/8; i++) {
            sb.append('\t').append(new String(columnValueArray, i*8, columnValueSizeArray[i]));
        }
//        for(ByteBuffer buffer : columnValue) {
//            sb.append('\t').append(new String(buffer.array(), 0, buffer.limit()));
//        }
        return sb.toString();
    }

    public int hashCode() {
        return (int)pk;
    }
}
