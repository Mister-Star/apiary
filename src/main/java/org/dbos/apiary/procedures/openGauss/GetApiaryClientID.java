package org.dbos.apiary.procedures.openGauss;

import org.dbos.apiary.openGauss.openGaussContext;
import org.dbos.apiary.openGauss.openGaussFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class GetApiaryClientID extends openGaussFunction {

    private static final String get = "SELECT Value from ApiaryMetadata WHERE Key=?;";
    private static final String insert = "INSERT INTO ApiaryMetadata(Key, Value) VALUES (?, ?);";
    private static final String update = "UPDATE ApiaryMetadata SET Value = ? WHERE Key = ?;";
    private static final String clientIDName = "ClientID";

    public static int runFunction(openGaussContext ctxt) throws SQLException {
        ResultSet r = (ResultSet) ctxt.executeQuery(get, clientIDName);
        int value;
        if (r.next()) {
            value = r.getInt(1);
        } else {
            value = 0;
        }
        if(value == 0) {
            ctxt.executeUpdate(insert, clientIDName, value + 1);
        }
        else {
            ctxt.executeUpdate(update, value + 1, clientIDName);
        }
        return value + 1;
    }
}