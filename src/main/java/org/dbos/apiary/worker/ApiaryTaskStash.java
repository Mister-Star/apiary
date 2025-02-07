package org.dbos.apiary.worker;

import org.dbos.apiary.function.ApiaryFuture;
import org.dbos.apiary.function.Task;
import org.zeromq.ZFrame;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

// This class is used to store the current execution progress of a called function.
public class ApiaryTaskStash {
    public final long callerId;
    public final long functionID;  // Task ID for itself.
    public final ZFrame replyAddr;
    public final Map<Long, Object> functionIDToValue;
    public final Queue<Task> queuedTasks;
    public final AtomicInteger numFinishedTasks = new AtomicInteger(0);
    public final long senderTimestampNano;
    public final String service;
    public final long execId;
    public final boolean isReplay;

    public int totalQueuedTasks;
    public Object output;

    public ApiaryTaskStash(String service, long execId, long callerId, long functionID, boolean isReplay, ZFrame replyAddr, long senderTimestampNano) {
        this.service = service;
        this.execId = execId;
        this.callerId = callerId;
        this.functionID = functionID;
        this.isReplay = isReplay;
        this.replyAddr = replyAddr;
        this.senderTimestampNano = senderTimestampNano;
        functionIDToValue = new ConcurrentHashMap<>();
        queuedTasks = new ConcurrentLinkedQueue<>();
        totalQueuedTasks = 0;
    }

    // If everything is resolved, then return the string value.
    // Otherwise, return null.
    Object getFinalOutput() {
        if (numFinishedTasks.get() == totalQueuedTasks) {
            if (output instanceof ApiaryFuture) {
                ApiaryFuture futureOutput = (ApiaryFuture) output;
                assert (functionIDToValue.containsKey(futureOutput.futureID));
                return functionIDToValue.get(futureOutput.futureID);
            } else {
                return output;
            }
        }
        return null;
    }
}
