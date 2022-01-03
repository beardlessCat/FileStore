package com.bigyj.store;

import com.bigyj.dispatcher.CommitLogDispatcher;

public class IndexFile implements CommitLogDispatcher {
    private static final String INDEX_FILE_PATH = "D://indexFile";
    private static final int COMMIT_LOG_FILE_SIZE = 0;

    //启动加载IndexFile文件
    public void load(){

    }

    @Override
    public void dispatcher() {

    }
}
