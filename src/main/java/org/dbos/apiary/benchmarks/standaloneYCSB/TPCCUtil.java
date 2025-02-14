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


package org.dbos.apiary.benchmarks.standaloneYCSB;

/*
 * jTPCCUtil - utility functions for the Open Source Java implementation of 
 *    the TPC-C benchmark
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import org.dbos.apiary.benchmarks.standaloneYCSB.pojo.Customer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class TPCCUtil {		
    /**
     * Creates a Customer object from the current row in the given ResultSet.
     * The caller is responsible for closing the ResultSet.
     * @param rs an open ResultSet positioned to the desired row
     * @return the newly created Customer object
     * @throws SQLException for problems getting data from row
     */
	public static Customer newCustomerFromResults(ResultSet rs)
			throws SQLException {
		Customer c = new Customer();
		// TODO: Use column indices: probably faster?
		c.c_w_id = rs.getInt("c_w_id");
		c.c_d_id = rs.getInt("c_d_id");
		c.c_id = rs.getInt("c_id");
		c.c_last = rs.getString("c_last");
		c.c_first = rs.getString("c_first");
		c.c_middle = rs.getString("c_middle");
		c.c_street_1 = rs.getString("c_street_1");
		c.c_street_2 = rs.getString("c_street_2");
		c.c_city = rs.getString("c_city");
		c.c_state = rs.getString("c_state");
		c.c_zip = rs.getString("c_zip");
		c.c_phone = rs.getString("c_phone");
		c.c_credit = rs.getString("c_credit");
		c.c_credit_lim = rs.getFloat("c_credit_lim");
		c.c_discount = rs.getFloat("c_discount");
		c.c_balance = rs.getFloat("c_balance");
		c.c_ytd_payment = rs.getFloat("c_ytd_payment");
		c.c_payment_cnt = rs.getInt("c_payment_cnt");
		c.c_delivery_cnt = rs.getInt("c_delivery_cnt");
		c.c_since = rs.getTimestamp("c_since");
		c.c_data = rs.getString("c_data");
		return c;
	}
	private static final RandomGenerator ran = new RandomGenerator(0);


	public static String randomStr(int strLen) {
	    if (strLen > 1) 
	        return ran.astring(strLen - 1, strLen - 1);
	    else
	        return "";
	} // end randomStr

	public static String randomNStr(int stringLength) {
		if (stringLength > 0)
		    return ran.nstring(stringLength, stringLength);
		else
		    return "";
	}

	public static String getCurrentTime() {
		return TPCCConfig.dateFormat.format(new java.util.Date());
	}

	public static String formattedDouble(double d) {
		String dS = "" + d;
		return dS.length() > 6 ? dS.substring(0, 6) : dS;
	}

	// TODO: TPCC-C 2.1.6: For non-uniform random number generation, the
	// constants for item id,
	// customer id and customer name are supposed to be selected ONCE and reused
	// for all terminals.
	// We just hardcode one selection of parameters here, but we should generate
	// these each time.
	private static final int OL_I_ID_C = 7911; // in range [0, 8191]
	private static final int C_ID_C = 259; // in range [0, 1023]
	// NOTE: TPC-C 2.1.6.1 specifies that abs(C_LAST_LOAD_C - C_LAST_RUN_C) must
	// be within [65, 119]
	private static final int C_LAST_LOAD_C = 157; // in range [0, 255]
	private static final int C_LAST_RUN_C = 223; // in range [0, 255]

	public static int getItemID(Random r) {
		return nonUniformRandom(8191, OL_I_ID_C, 1, TPCCConfig.configItemCount, r);
	}

	public static int getCustomerID(Random r) {
		return nonUniformRandom(1023, C_ID_C, 1, TPCCConfig.configCustPerDist, r);
	}

	public static String getLastName(int num) {
		return TPCCConfig.nameTokens[num / 100] + TPCCConfig.nameTokens[(num / 10) % 10]
				+ TPCCConfig.nameTokens[num % 10];
	}

	public static String getNonUniformRandomLastNameForRun(Random r) {
		return getLastName(nonUniformRandom(255, C_LAST_RUN_C, 0, 999, r));
	}

	public static String getNonUniformRandomLastNameForLoad(Random r) {
		return getLastName(nonUniformRandom(255, C_LAST_LOAD_C, 0, 999, r));
	}

	public static int randomNumber(int min, int max, Random r) {
		return (int) (r.nextDouble() * (max - min + 1) + min);
	}

	public static int nonUniformRandom(int A, int C, int min, int max, Random r) {
		return (((randomNumber(0, A, r) | randomNumber(min, max, r)) + C) % (max
				- min + 1))
				+ min;
	}

	public static AtomicLong customerIdCounter = new AtomicLong(0);
	public static AtomicLong districtIdCounter = new AtomicLong(0);
	public static AtomicLong historyIdCounter = new AtomicLong(0);
	public static AtomicLong itemIdCounter = new AtomicLong(0);
	public static AtomicLong neworderIdCounter = new AtomicLong(0);
	public static AtomicLong openorderIdCounter = new AtomicLong(0);
	public static AtomicLong orderlineIdCounter = new AtomicLong(0);
	public static AtomicLong stockIdCounter = new AtomicLong(0);
	public static AtomicLong warehouseIdCounter = new AtomicLong(0);


	public static Long nextId(String tableName) {
		StringBuilder builder = new StringBuilder();
		int tableIdx = 0;
		if (tableName.equals(TPCCConstants.TABLENAME_CUSTOMER)) {
			return customerIdCounter.incrementAndGet();
		} else if (tableName.equals(TPCCConstants.TABLENAME_DISTRICT)) {
			return districtIdCounter.incrementAndGet();
		} else if (tableName.equals(TPCCConstants.TABLENAME_HISTORY)) {
			return historyIdCounter.incrementAndGet();
		} else if (tableName.equals(TPCCConstants.TABLENAME_ITEM)) {
			return itemIdCounter.incrementAndGet();
		} else if (tableName.equals(TPCCConstants.TABLENAME_NEWORDER)) {
			return neworderIdCounter.incrementAndGet();
		} else if (tableName.equals(TPCCConstants.TABLENAME_OPENORDER)) {
			return openorderIdCounter.incrementAndGet();
		} else if (tableName.equals(TPCCConstants.TABLENAME_ORDERLINE)) {
			return orderlineIdCounter.incrementAndGet();
		} else if (tableName.equals(TPCCConstants.TABLENAME_STOCK)) {
			return stockIdCounter.incrementAndGet();
		} else if (tableName.equals(TPCCConstants.TABLENAME_WAREHOUSE)) {
			return warehouseIdCounter.incrementAndGet();
		} else {
			return 0l;
		}
    }
	public static AtomicLong idCounter = new AtomicLong(new Random().nextLong());
	public static Long makeHashedApiaryId(String tableName, Object... objs) {
		String s = makeApiaryId(tableName, objs);
		return idCounter.incrementAndGet() ^ s.hashCode();
	}

	public static String makeApiaryId(String tableName, Object... objs) {
		StringBuilder builder = new StringBuilder();
		int tableIdx = 0;
		if (tableName.equals(TPCCConstants.TABLENAME_CUSTOMER)) {
			tableIdx = 0;
		} else if (tableName.equals(TPCCConstants.TABLENAME_DISTRICT)) {
			tableIdx = 1;
		} else if (tableName.equals(TPCCConstants.TABLENAME_HISTORY)) {
			tableIdx = 2;
		} else if (tableName.equals(TPCCConstants.TABLENAME_ITEM)) {
			tableIdx = 3;
		} else if (tableName.equals(TPCCConstants.TABLENAME_NEWORDER)) {
			tableIdx = 4;
		} else if (tableName.equals(TPCCConstants.TABLENAME_OPENORDER)) {
			tableIdx = 5;
		} else if (tableName.equals(TPCCConstants.TABLENAME_ORDERLINE)) {
			tableIdx = 6;
		} else if (tableName.equals(TPCCConstants.TABLENAME_STOCK)) {
			tableIdx = 7;
		} else if (tableName.equals(TPCCConstants.TABLENAME_WAREHOUSE)) {
			tableIdx = 8;
		} else {
			tableIdx = 9;
		}
		builder.append(String.valueOf(tableIdx));
		for (int i = 0; i < objs.length; ++i) {
			builder.append('_');
			builder.append(String.valueOf(objs[i]));
		}
        return builder.toString();
    }

} // end jTPCCUtil
