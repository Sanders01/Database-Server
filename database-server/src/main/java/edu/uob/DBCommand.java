package edu.uob;

import edu.uob.Exceptions.CmdExecutionException;

import java.io.IOException;
import java.util.ArrayList;

public abstract class DBCommand {

    private DBServer server;

    protected String databaseName;
    protected String tableName;


    public abstract String executeQuery(DBServer server) throws CmdExecutionException, IOException;


}


