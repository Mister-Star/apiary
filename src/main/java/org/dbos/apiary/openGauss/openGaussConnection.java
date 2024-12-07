package org.dbos.apiary.openGauss;

import org.dbos.apiary.benchmarks.standalonetpcc.BenchmarkingExecutableServer;
import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.ScopedTimer;
import org.dbos.apiary.utilities.Tracer;
import org.postgresql.ds.common.BaseDataSource;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A connection to a Postgres database.
 */
public class openGaussConnection implements ApiaryConnection {
    private static final Logger logger = LoggerFactory.getLogger(openGaussConnection.class);

//    private final BaseDataSource ds;

    public final ThreadLocal<Connection> connection;
    private final ReadWriteLock activeTransactionsLock = new ReentrantReadWriteLock();
    private long biggestxmin = Long.MIN_VALUE;
    private final Set<TransactionContext> activeTransactions = ConcurrentHashMap.newKeySet();
    private final Set<TransactionContext> abortedTransactions = ConcurrentHashMap.newKeySet();
    private TransactionContext latestTransactionContext;
    public Tracer tracer = null;
    private final Properties dbProps;
    private final String database;

    public void setTracer(Tracer tracer) {
        this.tracer = tracer;
    }
    /**
     * Create a connection to a Postgres database.
     * @param hostname the Postgres database hostname.
     * @param port the Postgres database port.
     * @param databaseName the Postgres database name.
     * @param databaseUsername the Postgres database username.
     * @param databasePassword the Postgres database password.
     * @throws SQLException
     */
    public openGaussConnection(String hostname, Integer port, String databaseName, String databaseUsername, String databasePassword) throws SQLException {
        this.database = "jdbc:postgresql://" + hostname + ":" + port + "/" + databaseUsername;
        logger.info("openGauss Connection {}", database);
        this.dbProps = new Properties();
        this.dbProps.setProperty("user", databaseUsername);
        this.dbProps.setProperty("password", databasePassword);
        this.dbProps.setProperty("conn", database);
        this.dbProps.setProperty("driver", "org.postgresql.Driver");
        this.dbProps.setProperty("db", databaseName);

//        this.ds = new BaseDataSource() {
//            @Override
//            public String getDescription() {
//                return null;
//            }
//        }

        this.connection = ThreadLocal.withInitial(() -> {
           try {
               Connection conn = DriverManager.getConnection(database, dbProps);
               conn.setAutoCommit(false);
               if (ApiaryConfig.isolationLevel == ApiaryConfig.REPEATABLE_READ) {
                   conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
               } else if (ApiaryConfig.isolationLevel == ApiaryConfig.SERIALIZABLE) {
                   conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
               } else {
                   logger.info("Invalid isolation level: {}", ApiaryConfig.isolationLevel);
               }
               return conn;
           } catch (SQLException e) {
               e.printStackTrace();
           }
           return null;
        });
        try {
            Connection testConn = DriverManager.getConnection(database, dbProps);
            testConn.close();
        } catch (SQLException e) {
            logger.info("Failed to connect to Postgres");
            throw new RuntimeException("Failed to connect to Postgres");
        }
        createTable(ProvenanceBuffer.PROV_FuncInvocations,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID + " BIGINT NOT NULL, "
                + ProvenanceBuffer.PROV_APIARY_TIMESTAMP + " BIGINT NOT NULL, "
                + ProvenanceBuffer.PROV_EXECUTIONID + " BIGINT NOT NULL, "
                + ProvenanceBuffer.PROV_FUNCID + " BIGINT NOT NULL, "
                + ProvenanceBuffer.PROV_ISREPLAY + " SMALLINT NOT NULL, "
                + ProvenanceBuffer.PROV_SERVICE + " VARCHAR(1024) NOT NULL, "
                + ProvenanceBuffer.PROV_PROCEDURENAME + " VARCHAR(1024) NOT NULL");
        createTable(ProvenanceBuffer.PROV_ApiaryMetadata,
                "Key VARCHAR(1024) NOT NULL, Value Integer, PRIMARY KEY(key)");
        createTable(ProvenanceBuffer.PROV_QueryMetadata,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID + " BIGINT NOT NULL, "
                + ProvenanceBuffer.PROV_QUERY_SEQNUM + " BIGINT NOT NULL, "
                + ProvenanceBuffer.PROV_QUERY_STRING + " VARCHAR(2048) NOT NULL, "
                + ProvenanceBuffer.PROV_QUERY_TABLENAMES + " VARCHAR(1024) NOT NULL, "
                + ProvenanceBuffer.PROV_QUERY_PROJECTION + " VARCHAR(1024) NOT NULL "
        );
        // TODO: add back recorded outputs later for fault tolerance.
        // createTable("RecordedOutputs", "ExecID bigint, FunctionID bigint, StringOutput VARCHAR(1000), IntOutput integer, StringArrayOutput bytea, IntArrayOutput bytea, FutureOutput bigint, QueuedTasks bytea, PRIMARY KEY(ExecID, FunctionID)");
    }

    /**
     * Drop a table and its corresponding events table if they exist.
     * @param tableName the table to drop.
     * @throws SQLException
     */
    public void dropTable(String tableName) throws SQLException {
        Connection conn = DriverManager.getConnection(database, dbProps);
        Statement truncateTable = conn.createStatement();
        truncateTable.execute(String.format("DROP TABLE IF EXISTS %s;", tableName));
        truncateTable.execute(String.format("DROP TABLE IF EXISTS %sEvents;", tableName));
        truncateTable.close();
        conn.close();
    }

    /**
     * Create a table and a corresponding events table.
     * @param tableName the table to create.
     * @param specStr the schema of the table, in Postgres DDL.
     * @throws SQLException
     */
    public void createTable(String tableName, String specStr) throws SQLException {
        Connection conn = DriverManager.getConnection(database, dbProps);
        Statement s = conn.createStatement();
        s.execute(String.format("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, specStr));
        if (!specStr.contains("APIARY_TRANSACTION_ID")) {
            ResultSet r = s.executeQuery(String.format("SELECT * FROM %s", tableName));
            ResultSetMetaData rsmd = r.getMetaData();
            StringBuilder provTable = new StringBuilder(String.format(
                    "CREATE FOREIGN TABLE IF NOT EXISTS %sEvents (%s BIGINT NOT NULL, %s BIGINT NOT NULL, %s BIGINT NOT NULL, %s BIGINT NOT NULL",
                    tableName, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID,
                    ProvenanceBuffer.PROV_APIARY_TIMESTAMP, ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE,
                    ProvenanceBuffer.PROV_QUERY_SEQNUM));
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                provTable.append(",");
                provTable.append(rsmd.getColumnLabel(i + 1));
                provTable.append(" ");
                provTable.append(rsmd.getColumnTypeName(i + 1));
            }
            provTable.append(");");
            s.execute(provTable.toString());
        }
        s.close();
        conn.close();
    }

    public void createIndex(String indexString) throws SQLException {
        Connection c = DriverManager.getConnection(database, dbProps);
        Statement s = c.createStatement();
        s.execute(indexString);
        s.close();
        c.close();
    }

    private void rollback(openGaussContext ctxt) throws SQLException {
        abortedTransactions.add(ctxt.txc);
        for (String secondary : ctxt.secondaryWrittenKeys.keySet()) {
            Map<String, List<String>> updatedKeys = ctxt.secondaryWrittenKeys.get(secondary);
            ctxt.workerContext.getSecondaryConnection(secondary).rollback(updatedKeys, ctxt.txc);
        }
        ctxt.conn.rollback();
        abortedTransactions.remove(ctxt.txc);
        activeTransactions.remove(ctxt.txc);
    }

    @Override
    public FunctionOutput callFunction(String functionName, WorkerContext workerContext, String service, long execID,
                                       long functionID, boolean isReplay, Object... inputs) {
        long startTime = System.currentTimeMillis();
        Connection c = connection.get();
        FunctionOutput f = null;
        AtomicLong totalTime = new AtomicLong(0);
        openGaussContext succeededCtx = null;
        while (true) {
            try (ScopedTimer t = new ScopedTimer((long elapsed) -> totalTime.set(elapsed)) ) {
                activeTransactionsLock.readLock().lock();
                openGaussContext ctxt = new openGaussContext(c, workerContext, service, execID, functionID, isReplay,
                        new HashSet<>(activeTransactions), new HashSet<>(abortedTransactions));
                activeTransactions.add(ctxt.txc);
                latestTransactionContext = ctxt.txc;
                if (ctxt.txc.xmin > biggestxmin) {
                    biggestxmin = ctxt.txc.xmin;
                }
                activeTransactionsLock.readLock().unlock();
                try {
                    try (ScopedTimer t2 = new ScopedTimer((long elapsed) -> ctxt.executionNanos.set(elapsed)) ) {
                        f = workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
                    } catch (Exception e) {
                        throw e;
                    }
                    
                    boolean valid = true;
                    try (ScopedTimer t3 = new ScopedTimer((long elapsed) -> ctxt.validationNanos.set(elapsed)) ) {
                        for (String secondary : ctxt.secondaryWrittenKeys.keySet()) {
                            Map<String, List<String>> writtenKeys = ctxt.secondaryWrittenKeys.get(secondary);
                            if (!writtenKeys.isEmpty()) {
                                valid &= ctxt.workerContext.getSecondaryConnection(secondary).validate(writtenKeys, ctxt.txc);
                            }
                        }
                    } catch (Exception e) {
                        throw e;
                    }
                    
                    try (ScopedTimer t4 = new ScopedTimer((long elapsed) -> ctxt.commitNanos.set(elapsed)) ) {
                        if (valid) {
                            ctxt.conn.commit();
                            for (String secondary : ctxt.secondaryWrittenKeys.keySet()) {
                                Map<String, List<String>> writtenKeys = ctxt.secondaryWrittenKeys.get(secondary);
                                ctxt.workerContext.getSecondaryConnection(secondary).commit(writtenKeys, ctxt.txc);
                            }
                            activeTransactions.remove(ctxt.txc);
                            succeededCtx = ctxt;
                            long elapsedTime = (System.currentTimeMillis() - startTime);
                            BenchmarkingExecutableServer.transactionTimes.add(elapsedTime);
                            logger.info("Txn execution total time {}", elapsedTime);
                            break;
                        } else {
                            rollback(ctxt);
                        }
                    } catch (Exception e) {
                        throw e;
                    }
                } catch (Exception e) {
                    if (e instanceof InvocationTargetException) {
                        Throwable innerException = e;
                        while (innerException instanceof InvocationTargetException) {
                            InvocationTargetException i = (InvocationTargetException) innerException;
                            innerException = i.getCause();
                        }
                        if (innerException instanceof PSQLException) {
                            PSQLException p = (PSQLException) innerException;
                            if (p.getSQLState().equals(PSQLState.SERIALIZATION_FAILURE.getState())) {
                                try {
                                    rollback(ctxt);
                                    continue;
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }
                            } else {
                                logger.info("Unrecoverable Postgres error: {} {}", p.getMessage(), p.getSQLState());
                            }
                        }
                    }
                    logger.info("Unrecoverable error in function execution: {}", e.getMessage());
                    e.printStackTrace();
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (succeededCtx != null && tracer != null) {
            tracer.setTotalTime(execID, totalTime.get());
            tracer.setXDSTInitNanos(execID, succeededCtx.initializationNanos.get());
            tracer.setXDSTExecutionNanos(execID, succeededCtx.executionNanos.get());
            tracer.setXDSTValidationNanos(execID, succeededCtx.validationNanos.get());
            tracer.setXDSTCommitNanos(execID, succeededCtx.commitNanos.get());
            tracer.addCategoryValidId(functionName, execID);
        }
        return f;
    }

    @Override
    public Set<TransactionContext> getActiveTransactions() {
        activeTransactionsLock.writeLock().lock();
        Set<TransactionContext> txSnapshot = new HashSet<>(activeTransactions);
        if (txSnapshot.isEmpty()) {
            txSnapshot.add(new TransactionContext(0, biggestxmin, biggestxmin, new ArrayList<>()));
        }
        activeTransactionsLock.writeLock().unlock();
        return txSnapshot;
    }

    @Override
    public TransactionContext getLatestTransactionContext() {
        return latestTransactionContext;
    }

    @Override
    public void updatePartitionInfo() {
        // Nothing here.
        return;
    }

    @Override
    public int getNumPartitions() {
        return 1;
    }

    @Override
    public String getHostname(Object... input) {
        return "localhost";
    }

    @Override
    public Map<Integer, String> getPartitionHostMap() {
        return Map.of(0, "localhost");
    }

}
