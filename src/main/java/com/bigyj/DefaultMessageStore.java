package com.bigyj;

import com.bigyj.dispatcher.CommitLogDispatcher;
import com.bigyj.dispatcher.IndexFileDispatcher;
import com.bigyj.entity.DispatchRequest;
import com.bigyj.entity.SelectMappedBufferResult;
import com.bigyj.message.Message;
import com.bigyj.store.CommitLog;
import com.bigyj.store.ConsumeQueue;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
@Slf4j
public class DefaultMessageStore {

    private final CommitLog commitLog;

    private final ConsumeQueue consumeQueue;

    private ReputMessageService reputMessageService ;

    public DefaultMessageStore() {
        this.commitLog = new CommitLog();
        this.consumeQueue = new ConsumeQueue();
        reputMessageService = new ReputMessageService();
    }

    public void load(){
        //加载commitLog
        commitLog.load();
        //加载consumeQueue
        consumeQueue.load();
        //文件恢复
        recover();
    }

    //文件恢复
    private void recover() {
        //恢复consume文件，并获取consumeQueue中创建的消息分区索引的最大值
        long maxOffSet = this.consumeQueue.recoverConsumeQueue();
        //恢复commitLog
        this.commitLog.recover((int) maxOffSet);
    }

    public void start(){
        this.load();
        reputMessageService.run();
    }

    public void putMessage(Message message) {
        this.commitLog.putMessage(message);
    }

    public class ReputMessageService implements Runnable{

        private final LinkedList<CommitLogDispatcher> dispatcherList;
        private volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        public ReputMessageService() {
            this.dispatcherList = new LinkedList<>();
            this.dispatcherList.addLast(new ConsumeQueueDispatcher());
            this.dispatcherList.addLast(new IndexFileDispatcher());
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(1000);
                    this.doReput();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void doReput() {
            log.info("执行doReput方法");
            //判断是否有有新的消息需要写入
            if (this.reputFromOffset < DefaultMessageStore.this.commitLog.getIndexPosition()) {
                this.reputFromOffset = DefaultMessageStore.this.commitLog.getIndexPosition();
            }
            long maxOffset = DefaultMessageStore.this.commitLog.getMaxOffset();
            if(this.reputFromOffset<maxOffset){
                SelectMappedBufferResult selectMappedBufferResult = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                DispatchRequest dispatchRequest = DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(selectMappedBufferResult);
                if(dispatchRequest.isSuccess()){
                    dispatcherList.stream().forEach(commitLogDispatcher -> {
                        commitLogDispatcher.dispatcher(dispatchRequest);
                    });
                    this.reputFromOffset  += dispatchRequest.getSize();
                    DefaultMessageStore.this.commitLog.commitIndexPosition((int) reputFromOffset);
                }
            }
        }
    }

    public class ConsumeQueueDispatcher implements CommitLogDispatcher {
        @Override
        public void dispatcher(DispatchRequest dispatchRequest) {
            DefaultMessageStore.this.consumeQueue.buildConsumerLog(dispatchRequest);
        }
    }

}
