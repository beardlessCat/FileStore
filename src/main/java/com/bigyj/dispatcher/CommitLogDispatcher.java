package com.bigyj.dispatcher;

import com.bigyj.entity.DispatchRequest;

public interface CommitLogDispatcher {
    void dispatcher(DispatchRequest dispatchRequest);
}
