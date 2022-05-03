package org.dbos.apiary.procedures.postgres.retwis;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;

public class RetwisFollow extends PostgresFunction {
    private static final String addFollowee = "INSERT INTO RetwisFollowees(UserID, FolloweeID) VALUES (?, ?);";

    public static int runFunction(ApiaryStatefulFunctionContext ctxt, int userID, int followeeID) throws SQLException {
        ctxt.apiaryExecuteUpdate(addFollowee, userID, followeeID);
        return userID;
    }
}