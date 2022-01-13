package com.bigyj.entity;

import lombok.ToString;

@ToString
public class DispatchRequest {
    private  long offset ;

    public DispatchRequest(boolean success) {
        this.success = success;
    }

    private int size;
    private String tags ;
    private boolean success;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public DispatchRequest(long offset, int size, String tags, boolean success) {
        this.offset = offset;
        this.size = size;
        this.tags = tags;
        this.success = success;
    }

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

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public DispatchRequest(long offset, int size, String tags) {
        this.offset = offset;
        this.size = size;
        this.tags = tags;
    }
}
