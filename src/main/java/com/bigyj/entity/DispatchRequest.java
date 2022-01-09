package com.bigyj.entity;

import java.nio.ByteBuffer;

public class DispatchRequest {
    private  long offset ;
    private int size;
    private ByteBuffer byteBuffer ;

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public void setByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public DispatchRequest(long offset, int size, ByteBuffer byteBuffer) {
        this.offset = offset;
        this.size = size;
        this.byteBuffer = byteBuffer;
    }
}
