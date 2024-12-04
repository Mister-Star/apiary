package org.dbos.apiary.benchmarks.standaloneYCSB;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface CrossDBTransactionManager {
    public void begin();
    public void commit();
    public void rollback();
    public PreparedStatement getPreparedStatement(String DBType, String SQL) throws SQLException;
    public Connection getRawConnection(String DBType) throws SQLException;
}
