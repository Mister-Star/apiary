package org.dbos.apiary.benchmarks.standalonetpcc;

import org.dbos.apiary.benchmarks.standalonetpcc.procedures.StandaloneNewOrderFunction;
import org.dbos.apiary.benchmarks.standalonetpcc.procedures.StandalonePaymentFunction;
import org.dbos.apiary.benchmarks.standalonetpcc.procedures.StandaloneResultOutputAndClear;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryScheduler;
import org.dbos.apiary.worker.ApiaryWFQScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.xa.XAConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkingExecutableServer {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkingExecutableServer.class);

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
        apiaryWorker.registerFunction("StandaloneResultOutputAndClear", XAConfig.postgres, StandaloneResultOutputAndClear::new);



        apiaryWorker.startServing();

        logger.info("apiaryWorker Start!");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        System.err.println("Stopping Apiary worker server.");
        apiaryWorker.shutdown();
        }));
        Thread.sleep(Long.MAX_VALUE);
        apiaryWorker.shutdown();


    }
}