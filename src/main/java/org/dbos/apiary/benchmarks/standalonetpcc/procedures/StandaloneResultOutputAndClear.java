/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package org.dbos.apiary.benchmarks.standalonetpcc.procedures;

import org.apache.log4j.Logger;
import org.dbos.apiary.benchmarks.standalonetpcc.TPCCBenchmark;
import org.dbos.apiary.utilities.Percentile;
import org.dbos.apiary.xa.XAFunction;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class StandaloneResultOutputAndClear extends XAFunction {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(StandaloneResultOutputAndClear.class);

    private static final Collection<Long> paymentTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> newOrderTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> transactionTimes = new ConcurrentLinkedQueue<>();

    public static int runFunction(org.dbos.apiary.postgres.PostgresContext context, int elapsedTime, int threadNum) throws Exception {

        logger.info("=============================================================");
        logger.info("====================Server Side Info=========================");
        logger.info("=============================================================");

        List<Long> queryTimes = newOrderTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("New order transactions: Duration: {} ThreadNum: {} Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, threadNum, numQueries, String.format("%.03f", throughput), average, p50, p99);
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
            logger.info("Payment transactions: Duration: {} ThreadNum: {} Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, threadNum, numQueries, String.format("%.03f", throughput), average, p50, p99);
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
            logger.info("Total Operations: Duration: {} ThreadNum: {} Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, threadNum, numQueries, String.format("%.03f", throughput), average, p50, p99);
        }

        logger.info("=============================================================");
        logger.info("==========================End================================");
        logger.info("=============================================================");

        paymentTimes.clear();
        newOrderTimes.clear();
        transactionTimes.clear();

        return 0;
    }

    static public void addPaymentTime(long time) {
        paymentTimes.add(time);
    }

    static public void addNewOrderTime(long time) {
        newOrderTimes.add(time);
    }

    static public void addTransactionTime(long time) {
        transactionTimes.add(time);
    }



}