package com.bigyj.store;

import com.bigyj.entity.ConsumeLogIndexObject;
import com.bigyj.entity.DispatchRequest;
import com.bigyj.mmap.MappedFile;
import com.bigyj.mmap.MappedFileQueue;
import com.bigyj.util.MsgUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumeQueue {
    private static final String CONSUME_FILE_PATH = "D://store//consumeQueue";
    private static final int COMMIT_LOG_FILE_SIZE = 1024 * 64;
    private static final int CQ_STORE_UNIT_SIZE = 20;

    //记录消息数量
    private final AtomicInteger totalMessage = new AtomicInteger(0);

    private MappedFileQueue mappedFileQueue ;

    public ConsumeQueue() {
        this.mappedFileQueue = new MappedFileQueue(CONSUME_FILE_PATH,COMMIT_LOG_FILE_SIZE);
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

    public void setMappedFileQueue(MappedFileQueue mappedFileQueue) {
        this.mappedFileQueue = mappedFileQueue;
    }

    //启动加载CommitLog文件
    public void load(){
        this.mappedFileQueue.load();
    }

    //构建消息分区索引
    public void buildConsumerLog(DispatchRequest dispatchRequest){
        int size  =dispatchRequest.getSize();
        String tags = dispatchRequest.getTags();
        long offset = dispatchRequest.getOffset();
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if(mappedFile==null||mappedFile.isFull()){
            //创建一个新的文件
            mappedFile = this.mappedFileQueue.getLastMappedFile(0);
        }
        ByteBuffer byteBuffer  = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
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

    //恢复consumeQueue文件
    public long recoverConsumeQueue() {
        CopyOnWriteArrayList<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        long maxOffset = 0L;
        long indexOffSet = 0 ;
        for(int i = 0 ;i<mappedFiles.size();i++){
            MappedFile mappedFile = mappedFiles.get(i);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            //最后一个文件最大
            if(i == mappedFiles.size()-1){
                for (int j = 0; i < mappedFiles.size(); j += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();
                    indexOffSet =indexOffSet> offset?indexOffSet:offset;
                    //表示有数据
                    if (offset >= 0 && size > 0) {
                        maxOffset  = j + CQ_STORE_UNIT_SIZE;
                    }else {
                        //没数据表示已经加载到了文件位置
                        break;
                    }
                }
            }else {
                maxOffset = mappedFile.getFileSize();
            }
            maxOffset = mappedFile.getFileFromOffset() + maxOffset ;
            mappedFile.commit((int) maxOffset);
        }
        return indexOffSet ;
    }
}
