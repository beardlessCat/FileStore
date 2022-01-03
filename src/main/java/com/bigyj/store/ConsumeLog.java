package com.bigyj.store;

import com.bigyj.dispatcher.CommitLogDispatcher;
import com.bigyj.entity.ConsumeLogIndexObject;
import com.bigyj.mmap.MappedFile;
import com.bigyj.mmap.MappedFileQueue;
import com.bigyj.util.MsgUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumeLog implements CommitLogDispatcher {
    private static final String CONSUME_FILE_PATH = "D://consumeQueue";
    private static final int COMMIT_LOG_FILE_SIZE = 1024 * 64;
    private static final int CQ_STORE_UNIT_SIZE = 20;

    //记录消息数量
    private final AtomicInteger totalMessage = new AtomicInteger(0);

    private MappedFileQueue mappedFileQueue ;

    public ConsumeLog() {
        this.mappedFileQueue = new MappedFileQueue(CONSUME_FILE_PATH,COMMIT_LOG_FILE_SIZE);
    }

    //启动加载CommitLog文件
    public void load(){
        this.mappedFileQueue.load();
    }
    //构建消息分区索引
    void buildConsumerLog(long offset, int size, String tags){
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if(mappedFile==null||mappedFile.isFull()){
            //创建一个新的文件
            mappedFile = this.mappedFileQueue.getLastMappedFile(0);
        }
        ByteBuffer byteBuffer  = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.flip();
        byteBuffer.limit(CQ_STORE_UNIT_SIZE);
        //8个字节存放offset
        byteBuffer.putLong(offset);
        //4个字节存放消息长度
        byteBuffer.putInt(size);
        //8个字节存放offset
        byteBuffer.putLong(MsgUtils.tagsString2tagsCode(tags));
        //获取mappedFile
        mappedFile.appendMessage(byteBuffer.array());
        totalMessage.incrementAndGet();
    }

    //根据消息分区索引查询消息
    public ConsumeLogIndexObject getCommitLog(int startIndex){
        //先获取消息分区索引所在的文件
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(startIndex);
        //读取consumeLog中的数据，回去commitLog中消息的offSet及偏移量
        ByteBuffer byteBuffer = mappedFile.selectMappedBuffer(startIndex*20, 20);
        //读取offset
        long offsetToRead = byteBuffer.getLong();
        //读取消息长度
        int sizeToRead = byteBuffer.getInt();
        //读取tag
        long tagToRead = byteBuffer.getLong();
        return new ConsumeLogIndexObject(offsetToRead,sizeToRead,tagToRead);
    }

    @Override
    public void dispatcher() {

    }
}
