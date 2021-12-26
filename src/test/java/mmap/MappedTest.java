package mmap;

import com.bigyj.mmap.MappedFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
public class MappedTest {
    private static final String FILE_NAME = "D://000000";
    private static final int FILE_LENGTH = 1024 * 64;
    private static String CONSUME_QUEUE_FILE_NAME = "D://consumeQueue/000000";
    private MappedFile consumeQueueMappedFile ;
    private MappedFile mappedFile;
    private AtomicInteger consumeOffSet = new AtomicInteger(0);
    private List<Message> messageList = new ArrayList<>();
    private AtomicInteger totalMessage = new AtomicInteger(0);

    @BeforeEach
    void preTest(){
        messageList.add(new Message("Once","Once ,we were eager to be heroes of the world",1));
        messageList.add(new Message("Be","Be familiar with 300 Tang poems, and you can sing even if you can't write poems",2));
        messageList.add(new Message("hello","hello word",3));
        messageList.add(new Message("you","you and me",4));

    }

    @Test
    void testMappedCommitLog(){
        //创建mappedFile对象
        int offset = 0;
        mappedFile = new MappedFile(FILE_NAME, FILE_LENGTH);
        consumeQueueMappedFile = new MappedFile(CONSUME_QUEUE_FILE_NAME, FILE_LENGTH);
        for(Message message :messageList){
            byte[] bytes = message.getContent().getBytes(StandardCharsets.UTF_8);
            /**
             * 自定义协议
             * 消息id（4个字节），消息长度（8个字节），消息时间戳（8个字节），消息tag(8个字节)
             */
            //计算消息长度
            int messageSize =
                 4 //消息id
                +8 //消息长度
                +8 //当前时间戳
                +8 //消息tag
                +bytes.length;//消息内容
            //文件写入
            ByteBuffer byteBuffer  = ByteBuffer.allocate(messageSize);
            byteBuffer.putInt(message.getId());
            byteBuffer.putLong(bytes.length);
            byteBuffer.putLong(System.currentTimeMillis());
            byteBuffer.putLong(tagsString2tagsCode(message.getTags()));
            byteBuffer.put(bytes);

            mappedFile.appendMessage(byteBuffer.array());
            this.appendConsumerLog(offset,messageSize,message.getTags());
            offset+=messageSize;
        }
        getCommitLog();
    }

    //20个字节
    public static final int CQ_STORE_UNIT_SIZE = 20;

    void appendConsumerLog(long offset, int size, String tags){
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
    @AfterEach
    void decodeCQ() throws Exception {
        File consumeQueue = new File(CONSUME_QUEUE_FILE_NAME);
        FileInputStream fis = new FileInputStream(consumeQueue);
        DataInputStream dis = new DataInputStream(fis);
        long count = 1;
        while (true) {
            //读取offset
            long offset = dis.readLong();
            //读取消息长度
            int size = dis.readInt();
            //读取tag
            long tag = dis.readLong();
            if (size == 0) {
                break;
            }
            System.out.printf("%d: %d %d %d\n", count++,offset, size, tag );
        }
        fis.close();
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
            ByteBuffer commitLogInfo = mappedFile.selectMappedBuffer((int) offsetToRead, sizeToRead);
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
            consumeOffSet.incrementAndGet();
        }
    }
    public static long tagsString2tagsCode(final String tags) {
        if (null == tags || tags.length() == 0) { return 0; }
        int tagCode = tags.hashCode();
        return tagCode;
    }

    class Message{
        private String tags ;
        private String content;
        private int id ;

        public Message(String tags, String content, int id) {
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

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }
    }
}
