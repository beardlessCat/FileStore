package mmap;

import com.bigyj.mmap.MappedFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
public class MappedTest {
    private static final String FILE_NAME = "D://000000";
    private static final int FILE_LENGTH = 1024 * 64;
    private static final String CONSUME_QUEUE_FILE_NAME = "D://consumeQueue/000000";
    private static final String INDEX_FILE_NAME = "D://index/000000";
    private static final int INDEX_FILE_LENGTH = 40+50*4+50*36;

    private static final int hashSlotSize = 4;
    public static final int INDEX_HEADER_SIZE = 40;
    private final int  hashSlotNum = 50;
    private static final int indexSize = 4+8+4+8+4;

    //20个字节
    public static final int CQ_STORE_UNIT_SIZE = 20;
    private MappedFile consumeQueueMappedFile ;
    private MappedFile commitLogMappedFile;
    private MappedFile indexMappedFile;

    private final AtomicInteger consumeOffSet = new AtomicInteger(0);
    private final List<Message> messageList = new ArrayList<>();
    private final AtomicInteger totalMessage = new AtomicInteger(0);
    //index索引的数量
    private final AtomicInteger indexNum = new AtomicInteger(0);

    @BeforeEach
    void preTest(){
        messageList.add(new Message("Once","Once ,we were eager to be heroes of the world","1"));
        messageList.add(new Message("Be","Be familiar with 300 Tang poems, and you can sing even if you can't write poems","2"));
        messageList.add(new Message("hello","hello word", "3"));
        messageList.add(new Message("you","you and me","4"));
    }

    @Test
    void testMappedCommitLog() {
        //创建mappedFile对象
        int offset = 0;
        commitLogMappedFile = new MappedFile(FILE_NAME, FILE_LENGTH);
        consumeQueueMappedFile = new MappedFile(CONSUME_QUEUE_FILE_NAME, FILE_LENGTH);
        indexMappedFile = new MappedFile(INDEX_FILE_NAME, INDEX_FILE_LENGTH);

        for (Message message : messageList) {
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
            byteBuffer.putLong(tagsString2tagsCode(message.getTags()));
            byteBuffer.put(bytes);

            commitLogMappedFile.appendMessage(byteBuffer.array());
//            this.buildConsumerLog(offset,messageSize,message.getTags());
            this.buildIndex(offset, messageTime, message.getId(),messageSize);
            offset += messageSize;
        }
        getCommitLog();

        for (Message message : messageList) {
            Map<String, Object> offSet = getOffSet(message.getId());
            long phyOffset = (long) offSet.get("phyOffset");
            int size = (int) offSet.get("size");
            System.out.println("索引文件中取出的消息offSet为:"+offSet.get("phyOffset"));
            System.out.println("索引文件中取出的消息长度为:"+offSet.get("size"));
            getCommitLogMessageByOffsetAndSize((int) phyOffset,size);
        }
    }

    private void buildIndex(int phyOffset,long timestamp ,String key,int size) {
        System.out.println("存放的offSet为："+phyOffset);
        //indexHeader
        //8(beginTimestamp 8位long类型，索引文件构建第一个索引的消息落在broker的时间)+
        //8+(endTimestamp 8位long类型，索引文件构建最后一个索引消息落broker时间)+
        //8+(beginPhyOffset 8位long类型，索引文件构建第一个索引的消息commitLog偏移量)+
        //8+(endPhyOffset 8位long类型，索引文件构建最后一个索引消息commitLog偏移量)+
        //8+(hashSlotCount 4位int类型，构建索引占用的槽位数(这个值貌似没有具体作用))+
        //8+(indexCount 4位int类型，索引文件中构建的索引个数)+

        //indexSlot(4个字节)
        int keyHash = indexKeyHashMethod(key);
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


    void buildConsumerLog(long offset, int size, String tags){
        ByteBuffer byteBuffer  = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.flip();
        byteBuffer.limit(CQ_STORE_UNIT_SIZE);
        //8个字节存放offset
        byteBuffer.putLong(offset);
        //4个字节存放消息长度
        byteBuffer.putInt(size);
        //8个字节存放offset
        byteBuffer.putLong(tagsString2tagsCode(tags));
        //获取mappedFile
        consumeQueueMappedFile.appendMessage(byteBuffer.array());
        totalMessage.incrementAndGet();
    }


    public Map<String,Object> getOffSet(String key){
        //indexSlot(4个字节)
        int keyHash = indexKeyHashMethod(key);
        int slotPos = keyHash % hashSlotNum;
        int absSlotPos = INDEX_HEADER_SIZE + slotPos * hashSlotSize;
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

    public  void getCommitLog(){
        while ( consumeOffSet.get()<totalMessage.get()){
            //读取consumeLog中的数据，回去commitLog中消息的offSet及偏移量
            ByteBuffer byteBuffer = consumeQueueMappedFile.selectMappedBuffer(consumeOffSet.get()*20, 20);
            //读取offset
            long offsetToRead = byteBuffer.getLong();
            //读取消息长度
            int sizeToRead = byteBuffer.getInt();
            //读取tag
            long tagToRead = byteBuffer.getLong();
            getCommitLogMessageByOffsetAndSize((int) offsetToRead, sizeToRead);
            consumeOffSet.incrementAndGet();
        }
    }

    private void getCommitLogMessageByOffsetAndSize(int offsetToRead, int sizeToRead) {
        ByteBuffer commitLogInfo = commitLogMappedFile.selectMappedBuffer(offsetToRead, sizeToRead);
        //消息id
        int msgId = commitLogInfo.getInt();
        //消息长度
        long msgLength = commitLogInfo.getLong();
        //消息时间戳
        long msgTime = commitLogInfo.getLong();
        //消息tag
        long msgTag = commitLogInfo.getLong();
        //消息内容
        byte[] data = new byte[(int) msgLength];
        commitLogInfo.get(data,0, (int) msgLength);
        String msgContent = new String(data, StandardCharsets.UTF_8);
        System.out.printf("消息id:%d;\n消息长度:%d;\n消息时间:%d; \n消息标签：%d; \n消息内容：%s;\n================\n", msgId,msgLength, msgTime, msgTag,msgContent );
    }

    public static long tagsString2tagsCode(final String tags) {
        if (null == tags || tags.length() == 0) { return 0; }
        int tagCode = tags.hashCode();
        return tagCode;
    }


    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    class Message{
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
}
