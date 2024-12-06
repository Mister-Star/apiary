package org.dbos.apiary.benchmarks.standalonetpcc;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.dbos.apiary.benchmarks.standalonetpcc.procedures.*;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.utilities.Percentile;
import org.dbos.apiary.xa.*;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TPCCBenchmark {
    static public void printPercentile(Percentile p,String name) {
        logger.info("{}: count {}, avg latency {}, p50 latency {}, p75 latency {}, p90 latency {}, p95 latency {}, p99 latency {} ", p.size(), name, p.average(), p.nth(50), p.nth(75), p.nth(90), p.nth(95), p.nth(99));
    }
    private static final Logger logger = LoggerFactory.getLogger(TPCCBenchmark.class);

    private static String[] DBTypes = {XAConnection.MySQLDBType, XAConnection.PostgresDBType};

    // Use the following queues to record execution times.
    private static final Collection<Long> paymentTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> newOrderTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> transactionTimes = new ConcurrentLinkedQueue<>();

    static PGSimpleDataSource getPostgresDataSource(String hostname, Integer port, String databaseName, String databaseUsername, String databasePassword) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[] {hostname});
        ds.setPortNumbers(new int[] {port});
        ds.setDatabaseName(databaseName);
        ds.setUser(databaseUsername);
        ds.setPassword(databasePassword);
        ds.setSsl(false);
        return ds;
    }

    public static void benchmark(WorkloadConfiguration conf, String transactionManager, int threadNum, String mainHostAddr, Integer interval, Integer duration, int percentageNewOrder, boolean mysqlDelayLogFlush, boolean skipLoading, boolean skipBench) throws SQLException, InterruptedException, InvalidProtocolBufferException {
        logger.info("TPCC benchmark start");
        if (!skipLoading) {
            logger.info("start loading data");
            TPCCLoaderXDST loader = new TPCCLoaderXDST(conf,
            getPostgresDataSource(conf.getDBAddressPG(), XAConfig.postgresPort, conf.getDBName(), "postgres", "dbos"));
            List<LoaderThread> loaders = loader.createLoaderThreads();
            ThreadUtil.runNewPool(loaders, conf.getLoaderThreads());
        }

        if (skipLoading) {
            logger.info("TPCC data loading skipped");
        } else {
            logger.info("TPCC data loading finished");
        }
        
        if (skipBench) {
            logger.info("TPCC benchmark skipped");
            return;
        }

        PostgresConnection pconn;
        try {
            pconn = new PostgresConnection(conf.getDBAddressPG(), XAConfig.postgresPort, conf.getDBName(), "postgres", "dbos");
        } catch (Exception e) {
            logger.info("No MySQL/Postgres instance! {}", e.getMessage());
            return;
        }

        logger.info("Init clients num {} server ip {}", threadNum, mainHostAddr);
        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient(mainHostAddr));

        ExecutorService threadPool = Executors.newFixedThreadPool(threadNum);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (duration * 1000);
        AtomicBoolean warmed = new AtomicBoolean(false);
        AtomicBoolean stopped = new AtomicBoolean(false);
        AtomicBoolean success = new AtomicBoolean(true);

        AtomicInteger totalTasks = new AtomicInteger(0);

        Runnable r = () -> {
            if (stopped.get() == true) {
                return;
            }
            try {
                int chooser = ThreadLocalRandom.current().nextInt(100);
                int warehouseId = ThreadLocalRandom.current().nextInt(conf.getNumWarehouses()) + 1;
                long t0 = System.nanoTime();
                if (chooser < percentageNewOrder) {
                    client.get().executeFunction("StandaloneNewOrderFunction", warehouseId, conf.getNumWarehouses());
                    newOrderTimes.add(System.nanoTime() - t0);
                } else {
                    client.get().executeFunction("StandalonePaymentFunction", warehouseId, conf.getNumWarehouses());
                    paymentTimes.add(System.nanoTime() - t0);
                }
                transactionTimes.add(System.nanoTime() - t0);
                totalTasks.decrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        logger.info("start send transactions");
        long currentTime = System.currentTimeMillis();
        while (currentTime < endTime) {
            long t = System.nanoTime();
            threadPool.submit(r);
            totalTasks.incrementAndGet();
            while (totalTasks.get() >= threadNum) {
                if(System.currentTimeMillis() >= endTime) break;
                // Busy-spin
            }
            currentTime = System.currentTimeMillis();
        }
        long elapsedTime = (System.currentTimeMillis() - startTime);

        if (success.get()) {
            logger.info("All succeeded!");
        } else {
            logger.info("Inconsistency happened.");
        }
        
        List<Long> queryTimes = newOrderTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("New order transactions: Duration: {} ClientNum: {} Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, threadNum, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No new order transactions");
        }

        queryTimes = paymentTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Payment transactions: Duration: {} ClientNum: {} Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, threadNum, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No payment transactions");
        }

        queryTimes = transactionTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        numQueries = transactionTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Total Operations: Duration: {} ClientNum: {} Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, threadNum, numQueries, String.format("%.03f", throughput), average, p50, p99);
        }

        stopped.set(true);

        threadPool.shutdown();

        threadPool.awaitTermination(10000, TimeUnit.SECONDS);
        logger.info("All queries finished! {}", System.currentTimeMillis() - startTime);
    }
}
