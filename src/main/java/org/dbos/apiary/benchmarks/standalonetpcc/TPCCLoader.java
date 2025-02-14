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


package org.dbos.apiary.benchmarks.standalonetpcc;

/*
 * Copyright (C) 2004-2006, Denis Lussier
 *
 * LoadData - Load Sample Data directly into database tables or create CSV files for
 *            each table that can then be bulk loaded (again & again & again ...)  :-)
 *
 *    Two optional parameter sets for the command line:
 *
 *                 numWarehouses=9999
 *
 *                 fileLocation=c:/temp/csv/
 *
 *    "numWarehouses" defaults to "1" and when "fileLocation" is omitted the generated
 *    data is loaded into the database tables directly.
 *
 */

import org.apache.log4j.Logger;
import org.dbos.apiary.benchmarks.standalonetpcc.pojo.*;
import org.dbos.apiary.xa.BaseXAConnection;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * TPC-C Benchmark Loader
 */
public class TPCCLoader {
    private static final Logger LOG = Logger.getLogger(TPCCLoader.class);
	private final WorkloadConfiguration conf;
	private final Random rng = new Random();
	BaseXAConnection PostgresConnection;
    public TPCCLoader(WorkloadConfiguration conf, BaseXAConnection PostgresConnection, BaseXAConnection MySQLConnection) {
        this.conf = conf;
		this.PostgresConnection = PostgresConnection;
        numWarehouses = conf.getNumWarehouses();
        if (numWarehouses <= 0) {
            //where would be fun in that?
            numWarehouses = 1;
        }
    }

	public static Timestamp getTimestamp(long time) {
        Timestamp timestamp;
        
	// 2020-03-03: I am no longer aware of any DBMS that needs a specialized data type for timestamps.
        timestamp = new Timestamp(time);
        
        return (timestamp);
    }
    
    private int numWarehouses = 0;
    private static final int FIRST_UNPROCESSED_O_ID = 2101;
    
	public static String getDBType(int wid) {
		return TPCCConstants.DBTYPE_POSTGRES;
	}

    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<LoaderThread>();
        int numLoaders = 4;
        final CountDownLatch itemLatch = new CountDownLatch(numLoaders * 2);

        // ITEM
        // The ITEM table will be fully loaded before any other table.
        // Because the ITEM table is large (100k items per the TPC-C spec),
        // we divide the ITEM table across the maximum number of loader threads.
        for (int i = 1; i <= TPCCConfig.configItemCount;) {
            int numItemsPerLoader = TPCCConfig.configItemCount / numLoaders;
            int itemStartInclusive = i;
            int itemEndInclusive = Math.min(TPCCConfig.configItemCount, itemStartInclusive + numItemsPerLoader - 1);
			threads.add(new LoaderThread(PostgresConnection.getNewConnection(), "TPCC") {
                @Override
                public void load(Connection conn) throws SQLException {
                    loadItems(conn, itemStartInclusive, itemEndInclusive);
                    itemLatch.countDown();
                }
            });
            i = itemEndInclusive + 1;
        }
        
        // WAREHOUSES
        // We use a separate thread per warehouse. Each thread will load 
        // all of the tables that depend on that warehouse. They all have
        // to wait until the ITEM table is loaded first though.
        for (int w = 1; w <= numWarehouses; w++) {
            final int w_id = w;
			Connection rawConnection = null;
			rawConnection = PostgresConnection.getNewConnection();
            threads.add(new LoaderThread(rawConnection, "TPCC") {
                @Override
                public void load(Connection conn) throws SQLException {
                    // Make sure that we load the ITEM table first
                    try {
                        itemLatch.await();
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                        throw new RuntimeException(ex);
                    }

					if (LOG.isDebugEnabled()) LOG.debug("Starting to load WAREHOUSE " + w_id);
                    // WAREHOUSE
                    loadWarehouse(conn, w_id);
                    if (LOG.isDebugEnabled()) LOG.debug("WAREHOUSE " + w_id + " loaded");
                    // STOCK
                    loadStock(conn, w_id, TPCCConfig.configItemCount);
                    if (LOG.isDebugEnabled()) LOG.debug("WAREHOUSE " + w_id + " stock loaded");
                    // DISTRICT
                    loadDistricts(conn, w_id, TPCCConfig.configDistPerWhse);
                    if (LOG.isDebugEnabled()) LOG.debug("WAREHOUSE " + w_id + " district loaded");
                    // CUSTOMER
                    loadCustomers(conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
                    if (LOG.isDebugEnabled()) LOG.debug("WAREHOUSE " + w_id + " customer loaded");
                    // ORDERS
                    loadOrders(conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
					if (LOG.isDebugEnabled()) LOG.debug("WAREHOUSE " + w_id + " orders loaded");
                }
            });
        } // FOR
        return (threads);
    }

    protected void transRollback(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
        }
    }

    protected void transCommit(Connection conn) {
        try {
            conn.commit();
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            transRollback(conn);
        }
    }

    protected int loadItems(Connection conn, int itemStartInclusive, int itemEndInclusive) {
        int k = 0;
        int randPct = 0;
        int len = 0;
        int startORIGINAL = 0;
        boolean fail = false;

		final String itemInsertSQL = 
		"INSERT INTO " + TPCCConstants.TABLENAME_ITEM +
		" (__apiaryid__, I_ID, I_NAME, I_PRICE, I_DATA, I_IM_ID) " +
		" VALUES (?,?,?,?,?,?)";
	
        try {
            PreparedStatement itemPrepStmt = conn.prepareStatement(itemInsertSQL);

            Item item = new Item();
            int batchSize = 0;
            for (int i = itemStartInclusive; i <= itemEndInclusive; i++) {

                item.i_id = i;
                item.i_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24,
                        this.rng));
                item.i_price = (double) (TPCCUtil.randomNumber(100, 10000, this.rng) / 100.0);

                // i_data
                randPct = TPCCUtil.randomNumber(1, 100, this.rng);
                len = TPCCUtil.randomNumber(26, 50, this.rng);
                if (randPct > 10) {
                    // 90% of time i_data isa random string of length [26 .. 50]
                    item.i_data = TPCCUtil.randomStr(len);
                } else {
                    // 10% of time i_data has "ORIGINAL" crammed somewhere in
                    // middle
                    startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), this.rng);
                    item.i_data = TPCCUtil.randomStr(startORIGINAL - 1)
                            + "ORIGINAL"
                            + TPCCUtil.randomStr(len - startORIGINAL - 9);
                }

                item.i_im_id = TPCCUtil.randomNumber(1, 10000, this.rng);

                k++;

                int idx = 1;
				itemPrepStmt.setString(idx++, TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_ITEM, item.i_id));
                itemPrepStmt.setLong(idx++, item.i_id);
                itemPrepStmt.setString(idx++, item.i_name);
                itemPrepStmt.setDouble(idx++, item.i_price);
                itemPrepStmt.setString(idx++, item.i_data);
                itemPrepStmt.setLong(idx++, item.i_im_id);
                itemPrepStmt.addBatch();
                batchSize++;

                if (batchSize == TPCCConfig.configCommitCount) {
                    itemPrepStmt.executeBatch();
                    itemPrepStmt.clearBatch();
                    transCommit(conn);
                    batchSize = 0;
                }
            } // end for


            if (batchSize > 0) itemPrepStmt.executeBatch();
            transCommit(conn);

        } catch (BatchUpdateException ex) {
            SQLException next = ex.getNextException();
            LOG.error("Failed to load data for TPC-C", ex);
            if (next != null) LOG.error(ex.getClass().getSimpleName() + " Cause => " + next.getMessage());
            fail = true;
        } catch (SQLException ex) {
            SQLException next = ex.getNextException();
            LOG.error("Failed to load data for TPC-C", ex);
            if (next != null) LOG.error(ex.getClass().getSimpleName() + " Cause => " + next.getMessage());
            fail = true;
        } catch (Exception ex) {
            LOG.error("Failed to load data for TPC-C", ex);
            fail = true;
        } finally {
            if (fail) {
                LOG.debug("Rolling back changes from last batch");
                transRollback(conn);    
            }
        }

        return (k);

    } // end loadItem()

    protected int loadWarehouse(Connection conn, int w_id) {

		final String whInsertSQL = 
		"INSERT INTO " + TPCCConstants.TABLENAME_WAREHOUSE +
		" (__apiaryID__, W_ID, W_YTD, W_TAX, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP) " +
		" VALUES (?, ?,?,?,?,?,?,?,?,?)";

        try {
            PreparedStatement whsePrepStmt = conn.prepareStatement(whInsertSQL);
            Warehouse warehouse = new Warehouse();

            warehouse.w_id = w_id;
            warehouse.w_ytd = 300000;

            // random within [0.0000 .. 0.2000]
            warehouse.w_tax = (double) ((TPCCUtil.randomNumber(0, 2000, this.rng)) / 10000.0);
			warehouse.w_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, this.rng));
			warehouse.w_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, this.rng));
			warehouse.w_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, this.rng));
			warehouse.w_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, this.rng));
			warehouse.w_state = TPCCUtil.randomStr(3).toUpperCase();
			warehouse.w_zip = "123456789";

			int idx = 1;
			whsePrepStmt.setString(idx++, TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_WAREHOUSE, warehouse.w_id));
			whsePrepStmt.setLong(idx++, warehouse.w_id);
			whsePrepStmt.setDouble(idx++, warehouse.w_ytd);
			whsePrepStmt.setDouble(idx++, warehouse.w_tax);
			whsePrepStmt.setString(idx++, warehouse.w_name);
			whsePrepStmt.setString(idx++, warehouse.w_street_1);
			whsePrepStmt.setString(idx++, warehouse.w_street_2);
			whsePrepStmt.setString(idx++, warehouse.w_city);
			whsePrepStmt.setString(idx++, warehouse.w_state);
			whsePrepStmt.setString(idx++, warehouse.w_zip);
			whsePrepStmt.execute();

			transCommit(conn);
		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback(conn);
		} catch (Exception e) {
			e.printStackTrace();
			transRollback(conn);
		}

		return (1);

	} // end loadWhse()

	protected int loadStock(Connection conn, int w_id, int numItems) {

		int k = 0;
		int randPct = 0;
		int len = 0;
		int startORIGINAL = 0;

		final String stockInsertSQL = 
		"INSERT INTO " + TPCCConstants.TABLENAME_STOCK +
		" (__apiaryID__, S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10) " +
		" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		try {
		    PreparedStatement stckPrepStmt = conn.prepareStatement(stockInsertSQL);

			Stock stock = new Stock();
			for (int i = 1; i <= numItems; i++) {
				stock.s_i_id = i;
				stock.s_w_id = w_id;
				stock.s_quantity = TPCCUtil.randomNumber(10, 100, this.rng);
				stock.s_ytd = 0;
				stock.s_order_cnt = 0;
				stock.s_remote_cnt = 0;

				// s_data
				randPct = TPCCUtil.randomNumber(1, 100, this.rng);
				len = TPCCUtil.randomNumber(26, 50, this.rng);
				if (randPct > 10) {
					// 90% of time i_data isa random string of length [26 ..
					// 50]
					stock.s_data = TPCCUtil.randomStr(len);
				} else {
					// 10% of time i_data has "ORIGINAL" crammed somewhere
					// in middle
					startORIGINAL = TPCCUtil
							.randomNumber(2, (len - 8), this.rng);
					stock.s_data = TPCCUtil.randomStr(startORIGINAL - 1)
							+ "ORIGINAL"
							+ TPCCUtil.randomStr(len - startORIGINAL - 9);
				}

				k++;
				int idx = 1;
				stckPrepStmt.setString(idx++, TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_STOCK, stock.s_w_id, stock.s_i_id));
				stckPrepStmt.setLong(idx++, stock.s_w_id);
				stckPrepStmt.setLong(idx++, stock.s_i_id);
				stckPrepStmt.setLong(idx++, stock.s_quantity);
				stckPrepStmt.setDouble(idx++, stock.s_ytd);
				stckPrepStmt.setLong(idx++, stock.s_order_cnt);
				stckPrepStmt.setLong(idx++, stock.s_remote_cnt);
				stckPrepStmt.setString(idx++, stock.s_data);
				stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
				stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
				stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
				stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
				stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
				stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
				stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
				stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
				stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
				stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
				stckPrepStmt.addBatch();
				if ((k % TPCCConfig.configCommitCount) == 0) {
					stckPrepStmt.executeBatch();
					stckPrepStmt.clearBatch();
					transCommit(conn);
				}
			} // end for [i]

			stckPrepStmt.executeBatch();
			transCommit(conn);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback(conn);

		} catch (Exception e) {
			e.printStackTrace();
			transRollback(conn);
		}

		return (k);

	} // end loadStock()

	protected int loadDistricts(Connection conn, int w_id, int distWhseKount) {

		int k = 0;

		final String distInsertSQL = 
		"INSERT INTO " + TPCCConstants.TABLENAME_DISTRICT +
		" (__apiaryID__, D_W_ID, D_ID, D_YTD, D_TAX, D_NEXT_O_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP) " +
		" VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";

		try {

			PreparedStatement distPrepStmt = conn.prepareStatement(distInsertSQL);
			District district = new District();

			for (int d = 1; d <= distWhseKount; d++) {
				district.d_id = d;
				district.d_w_id = w_id;
				district.d_ytd = 30000;

				// random within [0.0000 .. 0.2000]
				district.d_tax = (float) ((TPCCUtil.randomNumber(0, 2000, this.rng)) / 10000.0);

				district.d_next_o_id = TPCCConfig.configCustPerDist + 1;
				district.d_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, this.rng));
				district.d_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, this.rng));
				district.d_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, this.rng));
				district.d_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, this.rng));
				district.d_state = TPCCUtil.randomStr(3).toUpperCase();
				district.d_zip = "123456789";

				k++;
				int idx = 1;
				distPrepStmt.setString(idx++, TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_DISTRICT, district.d_w_id, district.d_id));
				distPrepStmt.setLong(idx++, district.d_w_id);
				distPrepStmt.setLong(idx++, district.d_id);
				distPrepStmt.setDouble(idx++, district.d_ytd);
				distPrepStmt.setDouble(idx++, district.d_tax);
				distPrepStmt.setLong(idx++, district.d_next_o_id);
				distPrepStmt.setString(idx++, district.d_name);
				distPrepStmt.setString(idx++, district.d_street_1);
				distPrepStmt.setString(idx++, district.d_street_2);
				distPrepStmt.setString(idx++, district.d_city);
				distPrepStmt.setString(idx++, district.d_state);
				distPrepStmt.setString(idx++, district.d_zip);
				distPrepStmt.executeUpdate();
			} // end for [d]

			transCommit(conn);
		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback(conn);
		} catch (Exception e) {
			e.printStackTrace();
			transRollback(conn);
		}

		return (k);

	} // end loadDist()

	protected int loadCustomers(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

		int k = 0;

		final String custInsertSQL = 
		"INSERT INTO " + TPCCConstants.TABLENAME_CUSTOMER +
		" (__apiaryID__, C_W_ID, C_D_ID, C_ID, C_DISCOUNT, C_CREDIT, C_LAST, C_FIRST, C_CREDIT_LIM, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_STREET_1,C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_MIDDLE, C_DATA) " +
		" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
		final String histInsertSQL = 
		"INSERT INTO " + TPCCConstants.TABLENAME_HISTORY +
		" (__apiaryID__, H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " +
		" VALUES (?,?,?,?,?,?,?,?,?)";

		Customer customer = new Customer();
		History history = new History();

		try {
		    PreparedStatement custPrepStmt = conn.prepareStatement(custInsertSQL);
		    PreparedStatement histPrepStmt = conn.prepareStatement(histInsertSQL);

			for (int d = 1; d <= districtsPerWarehouse; d++) {
				for (int c = 1; c <= customersPerDistrict; c++) {
					Timestamp sysdate = this.getTimestamp(System.currentTimeMillis());

					customer.c_id = c;
					customer.c_d_id = d;
					customer.c_w_id = w_id;

					// discount is random between [0.0000 ... 0.5000]
					customer.c_discount = (float) (TPCCUtil.randomNumber(1, 5000, this.rng) / 10000.0);

					if (TPCCUtil.randomNumber(1, 100, this.rng) <= 10) {
						customer.c_credit = "BC"; // 10% Bad Credit
					} else {
						customer.c_credit = "GC"; // 90% Good Credit
					}
					if (c <= 1000) {
						customer.c_last = TPCCUtil.getLastName(c - 1);
					} else {
						customer.c_last = TPCCUtil.getNonUniformRandomLastNameForLoad(this.rng);
					}
					customer.c_first = TPCCUtil.randomStr(TPCCUtil.randomNumber(8, 16, this.rng));
					customer.c_credit_lim = 50000;

					customer.c_balance = -10;
					customer.c_ytd_payment = 10;
					customer.c_payment_cnt = 1;
					customer.c_delivery_cnt = 0;

					customer.c_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, this.rng));
					customer.c_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, this.rng));
					customer.c_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, this.rng));
					customer.c_state = TPCCUtil.randomStr(3).toUpperCase();
					// TPC-C 4.3.2.7: 4 random digits + "11111"
					customer.c_zip = TPCCUtil.randomNStr(4) + "11111";
					customer.c_phone = TPCCUtil.randomNStr(16);
					customer.c_since = sysdate;
					customer.c_middle = "OE";
					customer.c_data = TPCCUtil.randomStr(TPCCUtil
							.randomNumber(300, 500, this.rng));

					history.h_c_id = c;
					history.h_c_d_id = d;
					history.h_c_w_id = w_id;
					history.h_d_id = d;
					history.h_w_id = w_id;
					history.h_date = sysdate;
					history.h_amount = 10;
					history.h_data = TPCCUtil.randomStr(TPCCUtil
							.randomNumber(10, 24, this.rng));

					k = k + 2;
					int idx = 1;
					custPrepStmt.setString(idx++, TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_CUSTOMER, customer.c_w_id, customer.c_d_id, customer.c_id));
					custPrepStmt.setLong(idx++, customer.c_w_id);
					custPrepStmt.setLong(idx++, customer.c_d_id);
					custPrepStmt.setLong(idx++, customer.c_id);
					custPrepStmt.setDouble(idx++, customer.c_discount);
					custPrepStmt.setString(idx++, customer.c_credit);
					custPrepStmt.setString(idx++, customer.c_last);
					custPrepStmt.setString(idx++, customer.c_first);
					custPrepStmt.setDouble(idx++, customer.c_credit_lim);
					custPrepStmt.setDouble(idx++, customer.c_balance);
					custPrepStmt.setDouble(idx++, customer.c_ytd_payment);
					custPrepStmt.setLong(idx++, customer.c_payment_cnt);
					custPrepStmt.setLong(idx++, customer.c_delivery_cnt);
					custPrepStmt.setString(idx++, customer.c_street_1);
					custPrepStmt.setString(idx++, customer.c_street_2);
					custPrepStmt.setString(idx++, customer.c_city);
					custPrepStmt.setString(idx++, customer.c_state);
					custPrepStmt.setString(idx++, customer.c_zip);
					custPrepStmt.setString(idx++, customer.c_phone);
					custPrepStmt.setTimestamp(idx++, customer.c_since);
					custPrepStmt.setString(idx++, customer.c_middle);
					custPrepStmt.setString(idx++, customer.c_data);
					custPrepStmt.addBatch();

					idx = 1;
					histPrepStmt.setString(idx++, TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_HISTORY, history.h_c_id, history.h_c_d_id, history.h_c_w_id, history.h_d_id, history.h_w_id, history.h_date));
					histPrepStmt.setInt(idx++, history.h_c_id);
					histPrepStmt.setInt(idx++, history.h_c_d_id);
					histPrepStmt.setInt(idx++, history.h_c_w_id);
					histPrepStmt.setInt(idx++, history.h_d_id);
					histPrepStmt.setInt(idx++, history.h_w_id);
					histPrepStmt.setTimestamp(idx++, history.h_date);
					histPrepStmt.setDouble(idx++, history.h_amount);
					histPrepStmt.setString(idx++, history.h_data);
					histPrepStmt.addBatch();

					if ((k % TPCCConfig.configCommitCount) == 0) {
						custPrepStmt.executeBatch();
						histPrepStmt.executeBatch();
						custPrepStmt.clearBatch();
						custPrepStmt.clearBatch();
						transCommit(conn);
					}
				} // end for [c]
			} // end for [d]

			custPrepStmt.executeBatch();
			histPrepStmt.executeBatch();
			custPrepStmt.clearBatch();
			histPrepStmt.clearBatch();
			transCommit(conn);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback(conn);
		} catch (Exception e) {
			e.printStackTrace();
			transRollback(conn);
		}

		return (k);

	} // end loadCust()

	protected int loadOrders(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {
		final String orderInsertSQL = 
			"INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER +
			" (__apiaryID__, O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) " +
			" VALUES (?,?,?,?,?,?,?,?,?)";
	
		final String nworInsertSQL = 
			"INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER +
			" (__apiaryID__, NO_W_ID, NO_D_ID, NO_O_ID) " +
			" VALUES (?,?,?,?)";

		final String orlnInsertSQL = 
			"INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE +
			" (__apiaryID__, OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) " +
			" VALUES (?,?,?,?,?,?,?,?,?,?,?)";

		int k = 0;
		int t = 0;
		try {
		    PreparedStatement ordrPrepStmt = conn.prepareStatement(orderInsertSQL);
		    PreparedStatement nworPrepStmt = conn.prepareStatement(nworInsertSQL);
		    PreparedStatement orlnPrepStmt = conn.prepareStatement(orlnInsertSQL);

			Oorder oorder = new Oorder();
			NewOrder new_order = new NewOrder();
			OrderLine order_line = new OrderLine();

			for (int d = 1; d <= districtsPerWarehouse; d++) {
				// TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
				int[] c_ids = new int[customersPerDistrict];
				for (int i = 0; i < customersPerDistrict; ++i) {
					c_ids[i] = i + 1;
				}
				// Collections.shuffle exists, but there is no
				// Arrays.shuffle
				for (int i = 0; i < c_ids.length - 1; ++i) {
					int remaining = c_ids.length - i - 1;
					int swapIndex = this.rng.nextInt(remaining) + i + 1;
					assert i < swapIndex;
					int temp = c_ids[swapIndex];
					c_ids[swapIndex] = c_ids[i];
					c_ids[i] = temp;
				}

				int newOrderBatch = 0;
				for (int c = 1; c <= customersPerDistrict; c++) {

					oorder.o_id = c;
					oorder.o_w_id = w_id;
					oorder.o_d_id = d;
					oorder.o_c_id = c_ids[c - 1];
					// o_carrier_id is set *only* for orders with ids < 2101
					// [4.3.3.1]
					if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
						oorder.o_carrier_id = TPCCUtil.randomNumber(1, 10, this.rng);
					} else {
						oorder.o_carrier_id = null;
					}
					oorder.o_ol_cnt = TPCCUtil.randomNumber(5, 15, this.rng);
					oorder.o_all_local = 1;
					oorder.o_entry_d = this.getTimestamp(System.currentTimeMillis());

					k++;
					int idx = 1;
					ordrPrepStmt.setString(idx++, TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_OPENORDER, oorder.o_w_id, oorder.o_d_id, oorder.o_c_id, oorder.o_id));
					ordrPrepStmt.setInt(idx++, oorder.o_w_id);
		            ordrPrepStmt.setInt(idx++, oorder.o_d_id);
		            ordrPrepStmt.setInt(idx++, oorder.o_id);
		            ordrPrepStmt.setInt(idx++, oorder.o_c_id);
		            if (oorder.o_carrier_id != null) {
		                ordrPrepStmt.setInt(idx++, oorder.o_carrier_id);
		            } else {
		                ordrPrepStmt.setNull(idx++, Types.INTEGER);
		            }
		            ordrPrepStmt.setInt(idx++, oorder.o_ol_cnt);
		            ordrPrepStmt.setInt(idx++, oorder.o_all_local);
		            ordrPrepStmt.setTimestamp(idx++, oorder.o_entry_d);
		            ordrPrepStmt.addBatch();

					// 900 rows in the NEW-ORDER table corresponding to the last
					// 900 rows in the ORDER table for that district (i.e.,
					// with NO_O_ID between 2,101 and 3,000)
					if (c >= FIRST_UNPROCESSED_O_ID) {
						new_order.no_w_id = w_id;
						new_order.no_d_id = d;
						new_order.no_o_id = c;

						k++;
						idx = 1;
						nworPrepStmt.setString(idx++, TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_NEWORDER, new_order.no_w_id, new_order.no_d_id, new_order.no_o_id));
				        nworPrepStmt.setInt(idx++, new_order.no_w_id);
			            nworPrepStmt.setInt(idx++, new_order.no_d_id);
				        nworPrepStmt.setInt(idx++, new_order.no_o_id);
			            nworPrepStmt.addBatch();
						newOrderBatch++;
					} // end new order

					for (int l = 1; l <= oorder.o_ol_cnt; l++) {
						order_line.ol_w_id = w_id;
						order_line.ol_d_id = d;
						order_line.ol_o_id = c;
						order_line.ol_number = l; // ol_number
						order_line.ol_i_id = TPCCUtil.randomNumber(1,
						        TPCCConfig.configItemCount, this.rng);
						if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
							order_line.ol_delivery_d = oorder.o_entry_d;
							order_line.ol_amount = 0;
						} else {
							order_line.ol_delivery_d = null;
							// random within [0.01 .. 9,999.99]
							order_line.ol_amount = (float) (TPCCUtil.randomNumber(1, 999999, this.rng) / 100.0);
						}
						order_line.ol_supply_w_id = order_line.ol_w_id;
						order_line.ol_quantity = 5;
						order_line.ol_dist_info = TPCCUtil.randomStr(24);

						k++;
						idx = 1;
						orlnPrepStmt.setString(idx++, TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_ORDERLINE, order_line.ol_w_id, order_line.ol_d_id, order_line.ol_o_id, order_line.ol_number));
						orlnPrepStmt.setInt(idx++, order_line.ol_w_id);
			            orlnPrepStmt.setInt(idx++, order_line.ol_d_id);
			            orlnPrepStmt.setInt(idx++, order_line.ol_o_id);
			            orlnPrepStmt.setInt(idx++, order_line.ol_number);
			            orlnPrepStmt.setLong(idx++, order_line.ol_i_id);
			            if (order_line.ol_delivery_d != null) {
			                orlnPrepStmt.setTimestamp(idx++, order_line.ol_delivery_d);
			            } else {
			                orlnPrepStmt.setNull(idx++, 0);
			            }
			            orlnPrepStmt.setDouble(idx++, order_line.ol_amount);
			            orlnPrepStmt.setLong(idx++, order_line.ol_supply_w_id);
			            orlnPrepStmt.setDouble(idx++, order_line.ol_quantity);
			            orlnPrepStmt.setString(idx++, order_line.ol_dist_info);
			            orlnPrepStmt.addBatch();

						if ((k % TPCCConfig.configCommitCount) == 0) {
							ordrPrepStmt.executeBatch();
							if (newOrderBatch > 0) {
							    nworPrepStmt.executeBatch();
							    newOrderBatch = 0;
							}
							orlnPrepStmt.executeBatch();
							
							ordrPrepStmt.clearBatch();
							nworPrepStmt.clearBatch();
							orlnPrepStmt.clearBatch();
							transCommit(conn);
						}

					} // end for [l]

				} // end for [c]

			} // end for [d]


			if (LOG.isDebugEnabled())  LOG.debug("  Writing final records " + k + " of " + t);
		    ordrPrepStmt.executeBatch();
		    nworPrepStmt.executeBatch();
		    orlnPrepStmt.executeBatch();
			transCommit(conn);

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            se.printStackTrace();
            transRollback(conn);
        } catch (Exception e) {
            e.printStackTrace();
            transRollback(conn);
        }

        return (k);

    } // end loadOrder()

} // end LoadData Class
