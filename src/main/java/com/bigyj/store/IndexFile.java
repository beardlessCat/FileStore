package com.bigyj.store;

import com.bigyj.dispatcher.CommitLogDispatcher;
import com.bigyj.mmap.MappedFile;
import com.bigyj.mmap.MappedFileQueue;
import com.bigyj.util.MsgUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexFile implements CommitLogDispatcher {
    private static final String INDEX_FILE_PATH = "D://indexFile";
    private static final int INDEX_FILE_SIZE = 0;
    private MappedFileQueue mappedFileQueue ;
    private static final int hashSlotSize = 4;
    public static final int INDEX_HEADER_SIZE = 40;
    private final int  hashSlotNum = 50;
    private static final int indexSize = 4+8+4+8+4;
    //index索引的数量
    private final AtomicInteger indexNum = new AtomicInteger(0);
    public IndexFile() {
        this.mappedFileQueue = new MappedFileQueue(INDEX_FILE_PATH,INDEX_FILE_SIZE);
    }
    //启动加载IndexFile文件
    public void load(){
        this.mappedFileQueue.load();
    }

    private void buildIndex(int phyOffset,long timestamp ,String key,int size) {
        MappedFile indexMappedFile = this.mappedFileQueue.getLastMappedFile();
        if(indexMappedFile==null||indexMappedFile.isFull()){
            //创建一个新的文件
            indexMappedFile = this.mappedFileQueue.getLastMappedFile(0);
        }
        //indexHeader
        //8(beginTimestamp 8位long类型，索引文件构建第一个索引的消息落在broker的时间)+
        //8+(endTimestamp 8位long类型，索引文件构建最后一个索引消息落broker时间)+
        //8+(beginPhyOffset 8位long类型，索引文件构建第一个索引的消息commitLog偏移量)+
        //8+(endPhyOffset 8位long类型，索引文件构建最后一个索引消息commitLog偏移量)+
        //8+(hashSlotCount 4位int类型，构建索引占用的槽位数(这个值貌似没有具体作用))+
        //8+(indexCount 4位int类型，索引文件中构建的索引个数)+

        //indexSlot(4个字节)
        int keyHash = MsgUtils.indexKeyHashMethod(key);
        int slotPos = keyHash % hashSlotNum;
        int absSlotPos = INDEX_HEADER_SIZE + slotPos * hashSlotSize;
        //判断是否hash冲突
        int slotValue = indexMappedFile.getMappedByteBuffer().getInt(absSlotPos);
        if(slotValue<=0){

        }

        //indexLinkedList
        // 4（存储的是key的hash值）+
        // 8（存储的是消息在commitlog的物理偏移量phyOffset）+
        // 4（存储的是消息长度）+
        // 8（存储时间戳）+
        // 4（如果存在hash冲突，存储的是上一个消息的索引地址）
        //indexHeader的长度+哈希曹的长度+已经生成的indexLinkedList对象的长度

        int absIndexPos = INDEX_HEADER_SIZE +
                hashSlotNum*hashSlotSize +
                indexNum.get()*indexSize;
        indexMappedFile.getMappedByteBuffer().putInt(absIndexPos,keyHash);
        indexMappedFile.getMappedByteBuffer().putLong(absIndexPos+4,phyOffset);
        indexMappedFile.getMappedByteBuffer().putInt(absIndexPos+4+8,size);
        indexMappedFile.getMappedByteBuffer().putLong(absIndexPos+4+8+4,timestamp);
        indexMappedFile.getMappedByteBuffer().putInt(absIndexPos+4+8+4+8,slotValue);
        //indexSlot(4个字节),存储的事当前索引的创建的顺序，用于查询索引hash表
        indexMappedFile.getMappedByteBuffer().putInt(absSlotPos,indexNum.getAndIncrement());
    }

    public Map<String,Object> getOffSet(String key){
        //indexSlot(4个字节)
        int keyHash = MsgUtils.indexKeyHashMethod(key);
        int slotPos = keyHash % hashSlotNum;
        int absSlotPos = INDEX_HEADER_SIZE + slotPos * hashSlotSize;
        MappedFile indexMappedFile = null;
        //获取哈希槽中的值，即索引的创建的顺序序号
        int indexNum = indexMappedFile.getMappedByteBuffer().getInt(absSlotPos);
        //获取链表中的元素的存放索引的offSet. (indexHeader的长度+哈希曹的长度+已经生成的indexLinkedList对象的长度)
        int absIndexPos = INDEX_HEADER_SIZE +
                hashSlotNum*hashSlotSize +
                indexNum*indexSize;
        //indexLinkedList
        // 4（存储的是key的hash值）+
        // 8（存储的是消息在commitlog的物理偏移量phyOffset）+
        // 8（存储时间戳）
        // 4（如果存在hash冲突，存储的是上一个消息的索引地址）

        indexMappedFile.getMappedByteBuffer().getInt(absIndexPos);
        long phyOffset = indexMappedFile.getMappedByteBuffer().getLong(absIndexPos + 4);
        int size = indexMappedFile.getMappedByteBuffer().getInt(absIndexPos + 4 + 8);
        indexMappedFile.getMappedByteBuffer().getLong(absIndexPos+4+8+4);
        indexMappedFile.getMappedByteBuffer().getInt(absIndexPos+4+8+4+8);
        HashMap<String, Object> objectObjectHashMap = new HashMap<>();
        objectObjectHashMap.put("phyOffset",phyOffset);
        objectObjectHashMap.put("size",size);
        return objectObjectHashMap;
    }

    @Override
    public void dispatcher() {

    }
}
