package com.bigyj.mmap;

import com.bigyj.util.MsgUtils;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.CopyOnWriteArrayList;

public class MappedFileQueue {
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();
    private final String storePath;
    private final int mappedFileSize ;

    public CopyOnWriteArrayList<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public MappedFileQueue(String storePath, int mappedFileSize) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
    }

    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }

        return mappedFileLast;
    }

    /**
     * 启动是加载MappedFileQueue
     */
    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            Arrays.sort(files);
            for (File file : files) {
                MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
                mappedFile.commit(mappedFileSize);
                this.mappedFiles.add(mappedFile);
            }
        }

        return true;
    }

    public MappedFile getLastMappedFile(int startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    private MappedFile getLastMappedFile(int startOffset, boolean needCreate) {
        MappedFile lastMappedFile = null ;
        //获取文件名（初始化偏移量作为文件名）
        String fileName = MsgUtils.offset2FileName(startOffset);
        if(needCreate){
            lastMappedFile = new MappedFile(this.storePath + "/" + fileName, this.mappedFileSize);
            this.mappedFiles.add(lastMappedFile);
        }
        return lastMappedFile;
    }

    public MappedFile findMappedFileByOffset(long startIndex) {
        // fixme 暂不考虑文件删除的情况
        int index  = (int) (startIndex / mappedFileSize);
        return this.mappedFiles.get(index);
    }

    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;
        if(!this.mappedFiles.isEmpty()){
            mappedFileFirst = this.mappedFiles.get(0);
        }
        return mappedFileFirst ;
    }
}
