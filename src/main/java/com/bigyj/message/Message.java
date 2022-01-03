package com.bigyj.message;

public class Message {
    private String tags ;
    private String content;
    private String id ;

    public Message(String tags, String content, String id) {
        this.tags = tags;
        this.content = content;
        this.id = id;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
