package mmap;

import com.bigyj.DefaultMessageStore;
import com.bigyj.message.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class CommitLogTest {
    private final List<Message> messageList = new ArrayList<>();

    @BeforeEach
    void preTest(){
        messageList.add(new Message("Once","Once ,we were eager to be heroes of the world","1"));
        messageList.add(new Message("Be","Be familiar with 300 Tang poems, and you can sing even if you can't write poems","2"));
        messageList.add(new Message("hello","hello word", "3"));
        messageList.add(new Message("you","you and me","4"));
    }

    @Test
    void msgSendTest(){
        DefaultMessageStore defaultMessageStore = new DefaultMessageStore();
        defaultMessageStore.start();
        for (Message message : messageList) {
            // defaultMessageStore.putMessage(message);
        }
    }
}
