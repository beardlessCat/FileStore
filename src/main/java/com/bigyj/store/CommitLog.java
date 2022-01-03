package com.bigyj.store;


import com.bigyj.message.Message;
import com.bigyj.mmap.MappedFile;
import com.bigyj.mmap.MappedFileQueue;
import com.bigyj.util.MsgUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class CommitLog {
    private static final String COMMIT_LOG_FILE_PATH = "D://store//commitLog";
    private static final int COMMIT_LOG_FILE_SIZE = 1024 * 64;

    private MappedFileQueue mappedFileQueue ;

    public CommitLog() {
        this.mappedFileQueue = new MappedFileQueue(COMMIT_LOG_FILE_PATH,COMMIT_LOG_FILE_SIZE);
    }

    //启动加载CommitLog文件
    public void load(){
        this.mappedFileQueue.load();
    }
    //消息存储
    public void putMessage(Message message){
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if(mappedFile==null||mappedFile.isFull()){
            //创建一个新的文件
            mappedFile = this.mappedFileQueue.getLastMappedFile(0);
        }
        byte[] bytes = message.getContent().getBytes(StandardCharsets.UTF_8);
        /**
         * 自定义协议
         * 消息id（4个字节），消息长度（8个字节），消息时间戳（8个字节），消息tag(8个字节)
         */
        //计算消息长度
        int messageSize =
                4 //消息id
                + 8 //消息长度
                + 8 //当前时间戳
                + 8 //消息tag
                + bytes.length;//消息内容
        long messageTime = System.currentTimeMillis();
        //文件写入
        ByteBuffer byteBuffer = ByteBuffer.allocate(messageSize);
        byteBuffer.putInt(Integer.parseInt(message.getId()));
        byteBuffer.putLong(bytes.length);
        byteBuffer.putLong(messageTime);
        byteBuffer.putLong(MsgUtils.tagsString2tagsCode(message.getTags()));
        byteBuffer.put(bytes);
        mappedFile.appendMessage(byteBuffer.array());
    }

}
