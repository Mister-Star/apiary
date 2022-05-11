package org.dbos.apiary.function;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ApiaryContext provides APIs to invoke other functions and run queries.
 */
public abstract class ApiaryContext {

    private final AtomicInteger calledFunctionID = new AtomicInteger(0);
    private final List<Task> queuedTasks = new ArrayList<>();
    /**
     * Only for internal use. Do not use in public functions!
     */
    public final ProvenanceBuffer provBuff;
    /**
     * Only for internal use. Do not use in public functions!
     */
    public final String service;
    /**
     * Only for internal use. Do not use in public functions!
     */
    public final long execID, functionID;

    public ApiaryContext(ProvenanceBuffer provBuff, String service, long execID, long functionID) {
        this.provBuff = provBuff;
        this.service = service;
        this.execID = execID;
        this.functionID = functionID;
    }

    /** Public Interface for functions. **/

    /**
     * Queue a function for asynchronous execution.
     * This function synchronously queues the invoked function for later asynchronous execution.
     *
     * @param name      the name of the invoked function.
     * @param inputs    the list of arguments provided to the invoked function.
     * @return          an {@link ApiaryFuture} object that holds the future ID.
     */
    public ApiaryFuture apiaryQueueFunction(String name, Object... inputs) {
        long functionID = ((this.functionID + calledFunctionID.incrementAndGet()) << 4);
        Task futureTask = new Task(functionID, name, inputs);
        queuedTasks.add(futureTask);
        return new ApiaryFuture(functionID);
    }

    /**
     * Invoke a function synchronously and block waiting for the result.
     *
     * @param name      the name of the invoked function.
     * @param inputs    the list of arguments provided to the invoked function.
     * @return          an {@link FunctionOutput} object that stores the output from a function.
     */
    public abstract FunctionOutput apiaryCallFunction(String name, Object... inputs);

    /** Apiary-private **/

    /**
     * Only for internal use. Do not use in public functions!
     * @return {@link FunctionOutput}
     */
    public abstract FunctionOutput checkPreviousExecution();

    /**
     * Only for internal use. Do not use in public functions!
     * @param output    the finalized output of a function.
     */
    public abstract void recordExecution(FunctionOutput output);

    /**
     * Only for internal use. Do not use in public functions!
     * @param output    the original output of a function.
     * @return          the finalized output of a function.
     */
    public FunctionOutput getFunctionOutput(Object output) {
        return new FunctionOutput(output, queuedTasks);
    }
}