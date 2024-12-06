package org.dbos.apiary.benchmarks.standalonetpcc;

import org.dbos.apiary.benchmarks.standalonetpcc.procedures.StandaloneNewOrderFunction;
import org.dbos.apiary.benchmarks.standalonetpcc.procedures.StandalonePaymentFunction;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryScheduler;
import org.dbos.apiary.worker.ApiaryWFQScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.xa.XAConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class BenchmarkingExecutableServer {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkingExecutableServer.class);


    public static final Collection<Long> paymentTimes = new ConcurrentLinkedQueue<>();
    public static final Collection<Long> newOrderTimes = new ConcurrentLinkedQueue<>();
    public static final Collection<Long> transactionTimes = new ConcurrentLinkedQueue<>();


    public static int Output() throws Exception {

        logger.info("=============================================================");
        logger.info("====================Server Side Info=========================");
        logger.info("=============================================================");

        List<Long> queryTimes = newOrderTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("New order transactions: Queries: {} Average: {}μs p50: {}μs p99: {}μs", numQueries, average, p50, p99);
        } else {
            logger.info("No new order transactions");
        }

        queryTimes = paymentTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Payment transactions: Queries: {} Average: {}μs p50: {}μs p99: {}μs", numQueries, average, p50, p99);
        } else {
            logger.info("No payment transactions");
        }

        queryTimes = transactionTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        numQueries = transactionTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Total Operations:Queries: {} Average: {}μs p50: {}μs p99: {}μs", numQueries, average, p50, p99);
        }

        logger.info("=============================================================");
        logger.info("==========================End================================");
        logger.info("=============================================================");

        return 0;
    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting Apiary worker server. XDB transactions: {} Isolation level: {}",
                ApiaryConfig.XDBTransactions, ApiaryConfig.isolationLevel);
        org.apache.commons_voltpatches.cli.Options options = new org.apache.commons_voltpatches.cli.Options();
        options.addOption("db", true,
                "The secondary used by this worker.");
        options.addOption("s", true, "Which Scheduler?");
        options.addOption("t", true, "How many worker threads?");
        options.addOption("PostgresAddress", true, "Postgre Address.");

        org.apache.commons_voltpatches.cli.CommandLineParser parser = new org.apache.commons_voltpatches.cli.DefaultParser();
        org.apache.commons_voltpatches.cli.CommandLine cmd = parser.parse(options, args);

        ApiaryScheduler scheduler = new ApiaryNaiveScheduler();
        if (cmd.hasOption("s")) {
            if (cmd.getOptionValue("s").equals("wfq")) {
                logger.info("Using WFQ Scheduler");
                scheduler = new ApiaryWFQScheduler();
            } else if (cmd.getOptionValue("s").equals("naive")) {
                logger.info("Using Naive Scheduler");
                scheduler = new ApiaryNaiveScheduler();
            }
        }
        int numThreads = 64;
        if (cmd.hasOption("t")) {
            numThreads = Integer.parseInt(cmd.getOptionValue("t"));
        }
        logger.info("{} worker threads", numThreads);

        String postgresAddress = "localhost";
        if (cmd.hasOption("PostgresAddress")) {
            postgresAddress= cmd.getOptionValue("PostgresAddress");
        }
        logger.info("PostgresAddress {}", postgresAddress);
        ApiaryWorker apiaryWorker;

        apiaryWorker = new ApiaryWorker(scheduler, numThreads);
        PostgresConnection conn = null;
        try {
             conn = new PostgresConnection(postgresAddress, ApiaryConfig.postgresPort, "postgres", "postgres", "postgres");
        }
        catch (Exception e) {
            logger.info("Can not connect to Postgres {}", postgresAddress);
        }
        apiaryWorker.registerConnection(XAConfig.postgres, conn);
        apiaryWorker.registerFunction("StandalonePaymentFunction", XAConfig.postgres, StandalonePaymentFunction::new);
        apiaryWorker.registerFunction("StandaloneNewOrderFunction", XAConfig.postgres, StandaloneNewOrderFunction::new);

        apiaryWorker.startServing();

        logger.info("apiaryWorker Start!");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Output();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            System.err.println("Stopping Apiary worker server.");
        apiaryWorker.shutdown();
        }));
        Thread.sleep(Long.MAX_VALUE);
        apiaryWorker.shutdown();

    }
}