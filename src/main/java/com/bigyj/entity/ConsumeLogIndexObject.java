package com.bigyj.entity;

public class ConsumeLogIndexObject {
    //消息偏移量
    private long offset;
    //消息长度
    private int size ;
    //消息tag
    private long tag ;

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

    public long getTag() {
        return tag;
    }

    public void setTag(long tag) {
        this.tag = tag;
    }

    public ConsumeLogIndexObject(long offset, int size, long tag) {
        this.offset = offset;
        this.size = size;
        this.tag = tag;
    }
}
