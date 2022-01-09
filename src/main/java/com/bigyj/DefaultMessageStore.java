package com.bigyj;

import com.bigyj.dispatcher.CommitLogDispatcher;
import com.bigyj.dispatcher.ConsumeQueueDispatcher;
import com.bigyj.dispatcher.IndexFileDispatcher;
import com.bigyj.entity.DispatchRequest;
import com.bigyj.entity.SelectMappedBufferResult;
import com.bigyj.store.CommitLog;
import com.bigyj.store.ConsumeQueue;

import java.util.LinkedList;

public class DefaultMessageStore {

    private final CommitLog commitLog;

    private final ConsumeQueue consumeQueue;

    private ReputMessageService reputMessageService ;

    public DefaultMessageStore(CommitLog commitLog, ConsumeQueue consumeQueue) {
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
        this.commitLog.recover(maxOffSet);
    }

    public void start(){
        this.load();
        reputMessageService.run();
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
                    Thread.sleep(1);
                    this.doReput();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void doReput() {
            //判断是否有有新的消息需要写入
            if (this.reputFromOffset < DefaultMessageStore.this.commitLog.getMinOffset()) {
                this.reputFromOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            }
            if(this.reputFromOffset<DefaultMessageStore.this.commitLog.getMaxOffset()){
                SelectMappedBufferResult selectMappedBufferResult = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                DispatchRequest dispatchRequest = DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(selectMappedBufferResult);
                dispatcherList.stream().forEach(commitLogDispatcher -> {
                    commitLogDispatcher.dispatcher(dispatchRequest);
                    DefaultMessageStore.this.commitLog.commit(reputFromOffset);
                });
            }
        }
    }
}
