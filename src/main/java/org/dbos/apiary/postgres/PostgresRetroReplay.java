package org.dbos.apiary.postgres;

import org.dbos.apiary.ExecuteFunctionRequest;
import org.dbos.apiary.function.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class PostgresRetroReplay {
    private static final Logger logger = LoggerFactory.getLogger(PostgresRetroReplay.class);

    // Record a list of modified tables. Track dependencies for replaying requests.
    public static final Set<String> replayWrittenTables = ConcurrentHashMap.newKeySet();

    // Cache accessed tables of a function set. <firstFuncName, String[]>
    private static final Map<String, String[]> funcSetAccessTables = new ConcurrentHashMap<>();

    // Store currently unresolved tasks. <execId, funcId, task>
    public static final Map<Long, Map<Long, Task>> pendingTasks = new ConcurrentHashMap<>();

    // Store funcID to value mapping of each execution.
    public static final Map<Long, Map<Long, Object>> execFuncIdToValue = new ConcurrentHashMap<>();

    // Store execID to final output map. Because the output could be a future.
    // TODO: garbage collect this map.
    public static final Map<Long, Object> execIdToFinalOutput = new ConcurrentHashMap<>();

    // Store a list of skipped requests. Used for selective replay.
    private static final Set<Long> skippedExecIds = ConcurrentHashMap.newKeySet();

    // A pending commit map from original transaction ID to Postgres replay task.
    private static final Map<Long, PostgresReplayTask> pendingCommitTasks = new ConcurrentHashMap<>();

    private static final Collection<Long> commitTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> prepareTxnTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> submitTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> totalTimes = new ConcurrentLinkedQueue<>();

    private static void resetReplayState() {
        replayWrittenTables.clear();
        funcSetAccessTables.clear();
        pendingTasks.clear();
        execFuncIdToValue.clear();
        execIdToFinalOutput.clear();
        skippedExecIds.clear();
        pendingCommitTasks.clear();
        commitTimes.clear();
        prepareTxnTimes.clear();
        submitTimes.clear();
        totalTimes.clear();
    }

    public static Object retroExecuteAll(WorkerContext workerContext, long targetExecID, long endExecId, int replayMode) throws Exception {
        // Clean up.
        resetReplayState();

        long startTime = System.currentTimeMillis();
        if (replayMode == ApiaryConfig.ReplayMode.ALL.getValue()) {
            logger.debug("Replay the entire trace!");
        } else if (replayMode == ApiaryConfig.ReplayMode.SELECTIVE.getValue()) {
            logger.debug("Selective replay!");
        } else {
            logger.error("Do not support replay mode: {}", replayMode);
            return null;
        }
        assert(workerContext.provBuff != null);
        Connection provConn = ProvenanceBuffer.createProvConnection(workerContext.provDBType, workerContext.provAddress);

        // Find previous execution history, only execute later committed transactions or the ones failed due to unrecoverable issues (like constraint violations).
        String provQuery = String.format("SELECT %s, %s FROM %s WHERE %s = ? AND %s=0 AND %s=0 AND (%s=\'%s\' OR %s=\'%s\');",
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID,
                ApiaryConfig.tableFuncInvocations,
                ProvenanceBuffer.PROV_EXECUTIONID, ProvenanceBuffer.PROV_FUNCID,
                ProvenanceBuffer.PROV_ISREPLAY, ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_FAIL_UNRECOVERABLE);
        PreparedStatement stmt = provConn.prepareStatement(provQuery);
        stmt.setLong(1, targetExecID);
        ResultSet historyRs = stmt.executeQuery();
        long startTxId = -1;
        if (historyRs.next()) {
            startTxId = historyRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
            logger.debug("Replay start transaction ID: {}", startTxId);
        } else {
            logger.error("No corresponding original transaction for start execution {}", targetExecID);
            throw new RuntimeException("Cannot find original transaction!");
        }
        historyRs.close();

        // Find the transaction ID of the last execution. Only need to find the transaction ID of the first function.
        // Execute everything between [startTxId, endTxId)
        stmt.setLong(1, endExecId);
        ResultSet endRs = stmt.executeQuery();
        long endTxId = Long.MAX_VALUE;
        if (endRs.next()) {
            endTxId = endRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
            logger.debug("Replay end transaction ID (excluded): {}", endTxId);
        } else {
            logger.debug("No corresponding original transaction for end execution {}. Execute the entire trace!", endExecId);
        }

        // This query finds the starting order of transactions.
        // Replay mode only consider committed transactions.
        // APIARY_TRANSACTION_ID, APIARY_EXECUTIONID, APIARY_FUNCID, APIARY_PROCEDURENAME, APIARY_TXN_SNAPSHOT
        String startOrderQuery = String.format("SELECT %s, %s, %s, %s, %s FROM %s WHERE %s >= ? AND %s < ? AND %s=0 AND %s=\'%s\' ORDER BY %s;",
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID, ProvenanceBuffer.PROV_FUNCID, ProvenanceBuffer.PROV_PROCEDURENAME, ProvenanceBuffer.PROV_TXN_SNAPSHOT,
                ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID,
                ProvenanceBuffer.PROV_ISREPLAY,  ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        if (workerContext.hasRetroFunctions()) {
            // Include aborted transactions for retroaction.
            startOrderQuery = String.format("SELECT %s, %s, %s, %s, %s FROM %s WHERE %s >= ? AND %s < ? AND %s=0 AND (%s=\'%s\' OR %s=\'%s\') ORDER BY %s;",
                    ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID, ProvenanceBuffer.PROV_FUNCID, ProvenanceBuffer.PROV_PROCEDURENAME, ProvenanceBuffer.PROV_TXN_SNAPSHOT,
                    ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID,
                    ProvenanceBuffer.PROV_ISREPLAY,  ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                    ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_FAIL_UNRECOVERABLE,
                    ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        }
        PreparedStatement startOrderStmt = provConn.prepareStatement(startOrderQuery);
        startOrderStmt.setLong(1, startTxId);
        startOrderStmt.setLong(2, endTxId);
        ResultSet startOrderRs = startOrderStmt.executeQuery();

        // Collect a list of request IDs and their function names.
        List<PostgresReplayInfo> replayReqs = new ArrayList<>();
        while (startOrderRs.next()) {
            long resTxId = startOrderRs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
            long resExecId = startOrderRs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
            long resFuncId = startOrderRs.getLong(ProvenanceBuffer.PROV_FUNCID);
            String[] resNames = startOrderRs.getString(ProvenanceBuffer.PROV_PROCEDURENAME).split("\\.");
            String resName = resNames[resNames.length - 1]; // Extract the actual function name.
            String resSnapshotStr = startOrderRs.getString(ProvenanceBuffer.PROV_TXN_SNAPSHOT);
            replayReqs.add(new PostgresReplayInfo(resTxId, resExecId, resFuncId, resName, resSnapshotStr));
        }

        // This query finds the original input.
        String inputQuery = String.format("SELECT %s, r.%s, %s FROM %s AS r INNER JOIN %s as f ON r.%s = f.%s " +
                        "WHERE %s >= ? AND %s < ? AND %s = 0 AND %s = 0 AND (%s=\'%s\' OR %s=\'%s\') ORDER BY %s;",
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_EXECUTIONID,
                ProvenanceBuffer.PROV_REQ_BYTES, ApiaryConfig.tableRecordedInputs,
                ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_EXECUTIONID,
                ProvenanceBuffer.PROV_EXECUTIONID, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_FUNCID, ProvenanceBuffer.PROV_ISREPLAY,
                ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_COMMIT,
                ProvenanceBuffer.PROV_FUNC_STATUS, ProvenanceBuffer.PROV_STATUS_FAIL_UNRECOVERABLE,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID
        );
        Connection provInputConn = ProvenanceBuffer.createProvConnection(workerContext.provDBType, workerContext.provAddress);
        PreparedStatement inputStmt = provInputConn.prepareStatement(inputQuery);
        inputStmt.setLong(1, startTxId);
        inputStmt.setLong(2, endTxId);
        ResultSet inputRs = inputStmt.executeQuery();

        // Cache inputs of the original execution. <execId, input>
        long currInputExecId = -1;
        Object[] currInputs = null;

        long lastNonSkippedExecId = -1;  // The last not-skipped execution ID. Useful to decide the final output.

        // A connection pool to the backend database. For concurrent executions.
        Queue<Connection> connPool = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < workerContext.numWorkersThreads; i++) {
            connPool.add(workerContext.getPrimaryConnection().createNewConnection());
        }

        // A thread pool for concurrent function executions.
        ExecutorService threadPool = Executors.newFixedThreadPool(workerContext.numWorkersThreads);
        ExecutorService commitThreadPool = Executors.newCachedThreadPool();

        long prepTime = System.currentTimeMillis();
        logger.info("Prepare time: {} ms", prepTime - startTime);

        // Main loop: start based on the transaction ID order and the snapshot info, Postgres commit timestamp may not be reliable.
        // Start transactions based on their original txid order, but commit it before executing the first transaction that has it in the snapshot.
        // Maintain a pool of connections to the backend database to concurrently execute transactions.
        int totalStartOrderTxns = 0;
        int totalExecTxns = 0;
        List<Long> checkVisibleTxns = new ArrayList<>(); // Committed but not guaranteed to be visible yet.
        for (PostgresReplayInfo rpInfo : replayReqs) {
            long t0 = System.nanoTime();
            totalStartOrderTxns++;
            long resTxId = rpInfo.txnId;
            long resExecId = rpInfo.execId;
            long resFuncId = rpInfo.funcId;
            String resName = rpInfo.funcName;
            String resSnapshotStr = rpInfo.txnSnapshot;
            long xmax = PostgresUtilities.parseXmax(resSnapshotStr);
            List<Long> activeTxns = PostgresUtilities.parseActiveTransactions(resSnapshotStr);
            logger.debug("Processing txnID {}, execId {}, funcId {}, funcName {}", resTxId, resExecId, resFuncId, resName);

            // Check if we need to commit anything.
            Map<Long, Future<Long>> cleanUpTxns = new HashMap<>();
            for (long cmtTxn : pendingCommitTasks.keySet()) {
                PostgresReplayTask commitPgRpTask = pendingCommitTasks.get(cmtTxn);
                if (commitPgRpTask == null) {
                    logger.error("No task found for pending commit txn {}.", cmtTxn);
                    throw new RuntimeException("Failed to find commit transaction " + cmtTxn);
                }
                // If this transaction is in resTxId's snapshot, then wait and commit it.
                if ((cmtTxn < xmax) && !activeTxns.contains(cmtTxn)) {
                    logger.debug("Committing txnID {} because in the snapshot of txn {}", cmtTxn, resTxId);
                    Future<Long> cmtFut = commitThreadPool.submit(new PostgresCommitCallable(commitPgRpTask, workerContext, cmtTxn, replayMode, threadPool));
                    cleanUpTxns.put(cmtTxn, cmtFut);
                    // Use the new transaction ID! Not their original ones.
                    checkVisibleTxns.add(commitPgRpTask.replayTxnID);  // TODO: maybe only need to check for writes.
                } else if (workerContext.getFunctionReadOnly(commitPgRpTask.task.funcName)) {
                    // If it's a read-only transaction and has finished, but not in its snapshot, still release the resources immediately.
                    if (commitPgRpTask.resFut.isDone()) {
                        logger.debug("Clean up read-only txnID {} for current txnID {}", cmtTxn, resTxId);
                        cleanUpTxns.put(cmtTxn, CompletableFuture.completedFuture(cmtTxn));
                    }
                }
            }
            // Clean up pending commit map.
            for (long cmtTxn : cleanUpTxns.keySet()) {
                try {
                    long retVal = cleanUpTxns.get(cmtTxn).get(5, TimeUnit.SECONDS);
                    if (retVal != cmtTxn) {
                        logger.error("Commit failed. Commit txn: {}, retVal: {}", cmtTxn, retVal);
                    }
                } catch (TimeoutException e) {
                    logger.error("Commit timed out for transaction {}", cmtTxn);
                    // TODO: better error handling? Abort connection?
                }
                connPool.add(pendingCommitTasks.get(cmtTxn).conn);
                pendingCommitTasks.remove(cmtTxn);
            }
            long t1 = System.nanoTime();
            if (!cleanUpTxns.isEmpty()) {
                commitTimes.add(t1 - t0);
            }

            // Execute this transaction.

            // Get the input for this transaction.
            if ((resExecId != currInputExecId) && (resFuncId == 0L)) {
                // Read the input for this execution ID.
                if (inputRs.next()) {
                    currInputExecId = inputRs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
                    byte[] recordInput = inputRs.getBytes(ProvenanceBuffer.PROV_REQ_BYTES);
                    ExecuteFunctionRequest req = ExecuteFunctionRequest.parseFrom(recordInput);
                    currInputs = Utilities.getArgumentsFromRequest(req);
                    if (currInputExecId != resExecId) {
                        logger.error("Input execID {} does not match the expected ID {}!", currInputExecId, resExecId);
                        throw new RuntimeException("Retro replay failed due to mismatched IDs.");
                    }
                    logger.debug("Original arguments execid {}, inputs {}", currInputExecId, currInputs);
                } else {
                    logger.error("Could not find the input for this execution ID {} ", resExecId);
                    throw new RuntimeException("Retro replay failed due to missing input.");
                }
            }

            Task rpTask = new Task(resExecId, resFuncId, resName, currInputs);

            // Check if we can skip this function execution. If so, add to the skip list. Otherwise, execute the replay.
            boolean isSkipped = checkSkipFunc(workerContext, rpTask, skippedExecIds, replayMode, replayWrittenTables, funcSetAccessTables);

            if (isSkipped && (lastNonSkippedExecId != -1)) {
                // Do not skip the first execution.
                logger.debug("Skipping transaction {}, execution ID {}", resTxId, resExecId);
                skippedExecIds.add(resExecId);
                continue;
            }

            // Skip the task if it is absent. Because we allow reducing the number of called function. Should never have race condition because later tasks must have previous tasks in their snapshot.
            if ((rpTask.functionID > 0L) && (!pendingTasks.containsKey(rpTask.execId) || !pendingTasks.get(rpTask.execId).containsKey(rpTask.functionID))) {
                if (workerContext.hasRetroFunctions()) {
                    logger.debug("Skip execution ID {}, function ID {}, not found in pending tasks.", rpTask.execId, rpTask.functionID);
                } else {
                    logger.error("Not found execution ID {}, function ID {} in pending tasks. Should not happen in replay!", rpTask.execId, rpTask.functionID);
                }
                continue;
            }

            totalExecTxns++;
            lastNonSkippedExecId = resExecId;
            Connection currConn = connPool.poll();
            if (currConn == null) {
                // Allocate more connections.
                currConn = workerContext.getPrimaryConnection().createNewConnection();
            }
            PostgresReplayTask pgRpTask = new PostgresReplayTask(rpTask, currConn);

            // Because Postgres may commit a transaction but take a while for it to show up in the snapshot for the following transactions, wait until we get everything from checkVisibleTxns in the snapshot.
            // We can wait by committing the empty transaction and create a new pgCtxt.
            PostgresContext pgCtxt = new PostgresContext(pgRpTask.conn, workerContext, "retroReplay", pgRpTask.task.execId, pgRpTask.task.functionID, replayMode, new HashSet<>(), new HashSet<>(), new HashSet<>());
            boolean allVisible = false;
            while (!allVisible) {
                logger.debug("Checking visible transactions: {}. Current transaction id {}, xmin {}, xmax {}, active transactions {}", checkVisibleTxns.toString(), pgCtxt.txc.txID, pgCtxt.txc.xmin, pgCtxt.txc.xmax, pgCtxt.txc.activeTransactions.toString());
                allVisible = true;
                List<Long> visibleTxns = new ArrayList<>();
                for (long replayCmtTxn : checkVisibleTxns) {
                    // Check if the committed transaction does not show up in the snapshot.
                    if ((replayCmtTxn >= pgCtxt.txc.xmax) || pgCtxt.txc.activeTransactions.contains(replayCmtTxn)) {
                        logger.debug("Transaction {} still not visible. xmax {}, activetransactions: {}", replayCmtTxn, pgCtxt.txc.xmax, pgCtxt.txc.activeTransactions.toString());
                        allVisible = false;
                    } else {
                        visibleTxns.add(replayCmtTxn);  // Record visible.
                    }
                }
                checkVisibleTxns.removeAll(visibleTxns);
                if (!allVisible) {
                    try {
                        pgCtxt.conn.commit();
                        Thread.sleep(1); // Avoid busy loop.
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.error("Should not fail to commit an empty transaction.");
                        throw new RuntimeException("Should not fail to commit an empty transaction.");
                    }
                    // Start a new transaction and wait again.
                    pgCtxt = new PostgresContext(pgRpTask.conn, workerContext, "retroReplay", pgRpTask.task.execId, pgRpTask.task.functionID, replayMode, new HashSet<>(), new HashSet<>(), new HashSet<>());
                }
            }
            long t2 = System.nanoTime();
            prepareTxnTimes.add(t2 - t1);
            // Finally, launch this transaction but does not wait.
            pgRpTask.replayTxnID = pgCtxt.txc.txID;
            pgRpTask.resFut = threadPool.submit(new PostgresReplayCallable(pgCtxt, pgRpTask));
            pendingCommitTasks.put(resTxId, pgRpTask);
            long t3 = System.nanoTime();
            submitTimes.add(t3 - t2);
            totalTimes.add(t3 - t0);
        }

        if (!pendingCommitTasks.isEmpty()) {
            // Commit the rest.
            // The order doesn't matter because if they could commit originally, they must have no conflicts.
            // If they didn't commit originally, then the order also doesn't matter.
            for (long cmtTxn : pendingCommitTasks.keySet()) {
                long t1 = System.nanoTime();
                PostgresReplayTask commitPgRpTask = pendingCommitTasks.get(cmtTxn);
                if (commitPgRpTask == null) {
                    logger.error("No task found for pending commit txn {}.", cmtTxn);
                    throw new RuntimeException("Failed to find commit transaction " + cmtTxn);
                }
                logger.debug("Commit final pending txnID {}.", cmtTxn);
                Future<Long> cmtFut = commitThreadPool.submit(new PostgresCommitCallable(commitPgRpTask, workerContext, cmtTxn, replayMode, threadPool));
                cmtFut.get(5, TimeUnit.SECONDS);
                connPool.add(commitPgRpTask.conn);
                long t2 = System.nanoTime();
                commitTimes.add(t2 - t1);
            }
        }

        if (!pendingTasks.isEmpty() && workerContext.hasRetroFunctions()) {
            // Execute everything left to be processed, only in retro mode.
            // TODO: a better way to execute new tasks?
            logger.info("Process unfinished tasks.");
            Connection currConn = connPool.poll();
            for (long execId : pendingTasks.keySet()) {
                Map<Long, Task> execFuncs = pendingTasks.get(execId);
                while (!execFuncs.isEmpty()) {
                    for (long funcId : execFuncs.keySet()) {
                        totalStartOrderTxns++;
                        Task rpTask = execFuncs.get(funcId);
                        PostgresReplayTask pgRpTask = new PostgresReplayTask(rpTask, currConn);
                        PostgresContext pgCtxt = new PostgresContext(pgRpTask.conn, workerContext, "retroReplay", pgRpTask.task.execId, pgRpTask.task.functionID, replayMode, new HashSet<>(), new HashSet<>(), new HashSet<>());
                        pgRpTask.resFut = threadPool.submit(new PostgresReplayCallable(pgCtxt, pgRpTask));
                        Future<Long> cmtFut = commitThreadPool.submit(new PostgresCommitCallable(pgRpTask, workerContext, pgCtxt.txc.txID, replayMode, threadPool));
                        cmtFut.get(5, TimeUnit.SECONDS);
                    }
                }
            }
            connPool.add(currConn);
        }
        long elapsedTime = System.currentTimeMillis() - prepTime;

        logger.info("Last non skipped execId: {}", lastNonSkippedExecId);
        Object output = execIdToFinalOutput.get(lastNonSkippedExecId);  // The last non-skipped execution ID.
        String outputString = output.toString();
        if (output instanceof int[]) {
            outputString = Arrays.toString((int[]) output);
        } else if (output instanceof String[]){
            outputString = Arrays.toString((String[]) output);
        }
        logger.info("Final output: {} ...", outputString.substring(0, Math.min(outputString.length(), 100)));
        logger.info("Re-execution time: {} ms", elapsedTime);
        logger.info("Total original transactions: {}, re-executed transactions: {}", totalStartOrderTxns, totalExecTxns);
        printStats("Commit", commitTimes, elapsedTime);
        printStats("PrepareTxn", prepareTxnTimes, elapsedTime);
        printStats("SubmitTask", submitTimes, elapsedTime);
        printStats("TotalLoop", totalTimes, elapsedTime);

        // Clean up connection pool and statements.
        int totalNumConns = 0;
        while (!connPool.isEmpty()) {
            Connection currConn = connPool.poll();
            if (currConn != null) {
                currConn.close();
                totalNumConns++;
            }
        }
        logger.info("Total used connections: {}", totalNumConns);

        startOrderRs.close();
        startOrderStmt.close();
        stmt.close();
        inputRs.close();
        inputStmt.close();
        threadPool.shutdown();
        threadPool.awaitTermination(10, TimeUnit.SECONDS);
        commitThreadPool.shutdown();
        commitThreadPool.awaitTermination(10, TimeUnit.SECONDS);
        provConn.close();
        provInputConn.close();
        return output;
    }

    // Return true if the function execution can be skipped.
    private static boolean checkSkipFunc(WorkerContext workerContext, Task rpTask, Set<Long> skippedExecIds, int replayMode, Set<String> replayWrittenTables, Map<String, String[]> funcSetAccessTables) throws SQLException {
        if (replayMode == ApiaryConfig.ReplayMode.ALL.getValue()) {
            // Do not skip if we are replaying everything.
            return false;
        }

        logger.debug("Replay written tables: {}", replayWrittenTables.toString());

        // The current selective replay heuristic:
        // 1) If a request has been skipped, then all following functions will be skipped.
        // 2) If a function name is in the list of retroFunctions, then we cannot skip.
        // 3) Cannot skip a request if any of its function contains writes and touches write set.
        if (skippedExecIds.contains(rpTask.execId)) {
            return true;
        } else if (rpTask.functionID > 0) {
            // If a request wasn't skipped at the first function, then the following functions cannot be skipped as well.
            // Reduce the number of checks.
            return false;
        }

        // Always replay a request if it contains modified functions.
        List<String> funcSet = workerContext.getFunctionSet(rpTask.funcName);
        if (funcSet == null) {
            // Conservatively, replay it.
            logger.debug("Does not find function set info, cannot skip.");
            return false;
        }
        for (String funcName : funcSet) {
            if (workerContext.retroFunctionExists(funcName)) {
                logger.debug("Contains retro modified function {}, cannot skip.", funcName);
                return false;
            }
        }


        // Check if an execution contains any writes, check all downstream functions.
        boolean isReadOnly = workerContext.getFunctionSetReadOnly(rpTask.funcName);
        logger.debug("Function set {} isReadonly? {}", rpTask.funcName, isReadOnly);
        if (!isReadOnly) {
            // If a request contains write but has nothing to do with the related table, we can skip it.
            // Check query metadata table and see if any transaction related to this execution touches any written tables.
            String[] tables;
            if (funcSetAccessTables.containsKey(rpTask.funcName)) {
                tables = funcSetAccessTables.get(rpTask.funcName);
            } else {
                tables = workerContext.getFunctionSetAccessTables(rpTask.funcName);
                funcSetAccessTables.put(rpTask.funcName, tables);
            }
            if (tables.length < 1) {
                // Conservatively, cannot skip.
                return false;
            }
            for (String table : tables) {
                if (replayWrittenTables.contains(table)) {
                    logger.debug("Execution would touch table {} in the write set. Cannot skip.", table);
                    return false;
                }
            }

        }

        return true;
    }

    private static void printStats(String opName, Collection<Long> rawTimes, long elapsedTime) {
        List<Long> queryTimes = rawTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("[{}]: Duration: {}  Operations: {} Op/sec: {} Average: {}μs p50: {}μs p99: {}μs", opName, elapsedTime, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No {}.", opName);
        }
    }

}
