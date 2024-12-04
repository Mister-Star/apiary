package org.dbos.apiary.benchmarks.standalonetpcc;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.dbos.apiary.benchmarks.tpcc.procedures.XDSTNewOrderFunction;
import org.dbos.apiary.benchmarks.tpcc.procedures.XDSTPaymentFunction;
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
//        Options options = new Options();
//        options.addOption("b", true, "Which Benchmark?");
//        options.addOption("d", true, "Benchmark Duration (sec)?");
//        options.addOption("i", true, "Benchmark Interval (μs)");
//        options.addOption("mainHostAddr", true, "Address of the main Apiary host to connect to.");
//        options.addOption("mysqlAddr", true, "Address of the MySQL server.");
//        options.addOption("postgresAddr", true, "Address of the Postgres server.");
//        options.addOption("tm", true, "Which transaction manager to use? (bitronix | XDBT)");
//        options.addOption("w", true, "Number of warehouses?");
//        options.addOption("s", true, "Scale factor (Default: 1)");
//        options.addOption("l", true, "Number of loader threads (Default: half of the available processors)");
//        options.addOption("p", true, "Percentage of New-Order transactions (Default 50)");
//        options.addOption("df", false, "Delay log flushes till the end of validation in MySQL");
//        options.addOption("skipLoading", false, "Skip data loading");
//        options.addOption("skipBench", false, "Skip benchmark");
//        CommandLineParser parser = new DefaultParser();
//        CommandLine cmd = parser.parse(options, args);
//
//        int interval = 1000;
//        if (cmd.hasOption("i")) {
//            interval = Integer.parseInt(cmd.getOptionValue("i"));
//        }
//
//        int duration = 60;
//        if (cmd.hasOption("d")) {
//            duration = Integer.parseInt(cmd.getOptionValue("d"));
//        }
//
//        String mainHostAddr = "localhost";
//        if (cmd.hasOption("mainHostAddr")) {
//            mainHostAddr = cmd.getOptionValue("mainHostAddr");
//        }
//
//        String postgresAddr = "localhost";
//        if (cmd.hasOption("postgresAddr")) {
//            postgresAddr = cmd.getOptionValue("postgresAddr");
//        }
//
//        String mysqlAddr = "localhost";
//        if (cmd.hasOption("mysqlAddr")) {
//            mysqlAddr = cmd.getOptionValue("mysqlAddr");
//        }
//
//        String transactionManager = "bitronix";
//        if (cmd.hasOption("tm")) {
//            transactionManager = cmd.getOptionValue("tm");
//        }
//
//        WorkloadConfiguration conf = new WorkloadConfiguration();
//        conf.setBenchmarkName("TPCC");
//        conf.setDBName("tpcc");
//        conf.setDBAddressMySQL(mysqlAddr);
//        conf.setDBAddressPG(postgresAddr);
//        boolean mysqlDelayLogFlush = false;
//        boolean skipLoading = false;
//        boolean skipBench = false;
//        if (cmd.hasOption("df")) {
//            mysqlDelayLogFlush = true;
//        }
//        if (cmd.hasOption("skipLoading")) {
//            skipLoading = true;
//        }
//        if (cmd.hasOption("skipBench")) {
//            skipBench = true;
//        }
//        if (cmd.hasOption("s")) {
//            double scaleFactor = Double.parseDouble(cmd.getOptionValue("s"));
//            conf.setScaleFactor(scaleFactor);
//        } else {
//            conf.setScaleFactor(1);
//        }
//        if (cmd.hasOption("w")) {
//            int warehouses = Integer.parseInt(cmd.getOptionValue("w"));
//            conf.setNumWarehouses(warehouses);
//        } else {
//            conf.setNumWarehouses(20);
//        }
//        if (cmd.hasOption("l")) {
//            conf.setLoaderThreads(Integer.parseInt(cmd.getOptionValue("l")));
//        } else {
//            if (ThreadUtil.availableProcessors() / 2 < 1) {
//                conf.setLoaderThreads(1);
//            } else {
//                conf.setLoaderThreads(ThreadUtil.availableProcessors());
//            }
//        }

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
        apiaryWorker.registerFunction("XDSTPaymentFunction", XAConfig.postgres, XDSTPaymentFunction::new);
        apiaryWorker.registerFunction("XDSTNewOrderFunction", XAConfig.postgres, XDSTNewOrderFunction::new);


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