package com.bigyj.store;


import com.bigyj.entity.ConsumeLogIndexObject;
import com.bigyj.entity.DispatchRequest;
import com.bigyj.entity.SelectMappedBufferResult;
import com.bigyj.message.Message;
import com.bigyj.mmap.MappedFile;
import com.bigyj.mmap.MappedFileQueue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CommitLog {
    private static final String COMMIT_LOG_FILE_PATH = "D://store//commitLog";
    private static final int COMMIT_LOG_FILE_SIZE = 1024 * 64;
    private MappedFileQueue mappedFileQueue ;
    //已经在consumeQueue简历索引的最大消息的offSet
    protected final AtomicInteger indexPosition = new AtomicInteger(0);

    private ConcurrentHashMap<String,Long> topicQueueTable = new ConcurrentHashMap<>();
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
        ByteBuffer sliceByteBuffer = mappedFile.getMappedByteBuffer();
        byte[] bytes = message.getContent().getBytes(StandardCharsets.UTF_8);
        byte[] tagsBytes = message.getTags().getBytes(StandardCharsets.UTF_8);
        /**
         * 自定义协议
         * 消息id（4个字节），消息长度（8个字节），消息时间戳（8个字节），消息tag(8个字节)
         */
        //计算消息长度
        int messageSize =
                4 //消息id
                + 8 //消息总长度
                + 8 // 消息consumeQueue offSet
                + 8 // 消息offset PHYSICALOFFSET
                + 8 //当前时间戳
                + 8 //消息tag长度
                + tagsBytes.length //消息tag内容
                + 8 //消息内容长度
                + bytes.length;//消息内容
        ByteBuffer byteBuffer = ByteBuffer.allocate(messageSize);
        long messageTime = System.currentTimeMillis();
        String key = "0001";
        Long queueOffset = topicQueueTable.get(key);
        if (null == queueOffset) {
            queueOffset = 0L;
            this.topicQueueTable.put(key, queueOffset);
        }
        //文件写入
        //1.消息id
        byteBuffer.putInt(Integer.parseInt(message.getId()));
        //2.消息总长度
        byteBuffer.putLong(messageSize);
        //3.消息consumeQueue offSet
        byteBuffer.putLong(queueOffset);
        //4.消息offset= position + fileFromOffset
        long offset = sliceByteBuffer.position() + mappedFile.getFileFromOffset();
        byteBuffer.putLong(offset);
        //5.消息时间戳
        byteBuffer.putLong(messageTime);
        //6.消息tag长度
        byteBuffer.putLong(tagsBytes.length);
        //7.消息tag内容
        byteBuffer.put(tagsBytes);
        //8.消息内容长度
        byteBuffer.putLong(bytes.length);
        //9.消息内容
        byteBuffer.put(bytes);
        mappedFile.appendMessage(byteBuffer.array());
        this.topicQueueTable.put(key, queueOffset++);
        this.commit(offset);
    }

    Message getMessageByConsumeLog(ConsumeLogIndexObject consumeLog){
        MappedFile lastMappedFile = this.mappedFileQueue.findMappedFileByOffset(consumeLog.getOffset());
        ByteBuffer commitLogInfo = lastMappedFile.selectMappedBuffer((int) consumeLog.getOffset(), consumeLog.getSize());
        //1.消息id
        int msgId = commitLogInfo.getInt();
        //2.消息总长度
        long msgLength = commitLogInfo.getLong();
        //3.消息consumeQueue offSet
        long consumeQueueOffset = commitLogInfo.getLong();
        //4.消息offset= position + fileFromOffset
        long  physicalOffset = commitLogInfo.getLong();
        //5.消息时间戳
        long msgTime = commitLogInfo.getLong();
        //6.消息tag长度
        long tagLength = commitLogInfo.getLong();
        //7.消息tag内容
        byte[] tagBytes = new byte[(int) tagLength];
        commitLogInfo.get(tagBytes,0, (int) msgLength);
        String tags = new String(tagBytes, StandardCharsets.UTF_8);
        //8.消息内容长度
        long msgContentLength = commitLogInfo.getLong();
        //9.消息内容
        byte[] data = new byte[(int) msgContentLength];
        commitLogInfo.get(data,0, (int) msgContentLength);
        String msgContent = new String(data, StandardCharsets.UTF_8);
        System.out.printf("消息id:%d;\n消息长度:%d;\n消息时间:%d; \n消息标签：%d; \n消息内容：%s;\n================\n", msgId,msgLength, msgTime, msgContentLength,msgContent );
        return new Message(tags,msgContent,String.valueOf(msgId));
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public SelectMappedBufferResult getData(long offset) {
        int mappedFileSize = COMMIT_LOG_FILE_SIZE;
        //根据offset 获取mappedFile
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
        if (mappedFile != null) {
            //获取再commitLog中的物理偏移量
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos);
        }
        return null;
    }

    public void commit(long offset) {
        //根据offset 获取mappedFile
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
        if (mappedFile != null) {
           mappedFile.commit((int) offset);
        }
    }

    //commitLog文件恢复
    public void recover(int maxOffSet) {
        this.commitIndexPosition(maxOffSet);
    }

    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        return mappedFile.getFileFromOffset();
    }

    //根据传入的对象，选择第一条消息
    public DispatchRequest checkMessageAndReturnSize(SelectMappedBufferResult selectMappedBufferResult) {
        ByteBuffer commitLogInfo = selectMappedBufferResult.getByteBuffer();
        //1.消息id
        int msgId = commitLogInfo.getInt();
        //2.消息总长度
        long msgLength = commitLogInfo.getLong();
        if(msgLength == 0){
            return new DispatchRequest(false);
        }
        //3.消息consumeQueue offSet
        long consumeQueueOffset = commitLogInfo.getLong();
        //4.消息offset= position + fileFromOffset
        long  physicalOffset = commitLogInfo.getLong();
        //5.消息时间戳
        long msgTime = commitLogInfo.getLong();
        //6.消息tag长度
        long tagLength = commitLogInfo.getLong();
        //7.消息tag内容
        byte[] tagBytes = new byte[(int) tagLength];
        commitLogInfo.get(tagBytes,0, (int) tagLength);
        String tags = new String(tagBytes, StandardCharsets.UTF_8);
        //8.消息内容长度
        long msgContentLength = commitLogInfo.getLong();
        //9.消息内容
        byte[] data = new byte[(int) msgContentLength];
        commitLogInfo.get(data,0, (int) msgContentLength);
        String msgContent = new String(data, StandardCharsets.UTF_8);
        return new DispatchRequest(physicalOffset, (int) msgLength,tags,true) ;
    }

    public void commitIndexPosition(int indexedOffSet){
        this.indexPosition.set(indexedOffSet);
    }

    public int getIndexPosition(){
        return this.indexPosition.get();
    }
}
