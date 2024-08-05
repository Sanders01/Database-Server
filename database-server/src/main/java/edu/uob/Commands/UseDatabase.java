package edu.uob.Commands;

import edu.uob.DBCommand;
import edu.uob.DBServer;
import edu.uob.Exceptions.CmdExecutionException;

public class UseDatabase extends DBCommand {
    private final String databaseName;

    public UseDatabase(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }



    @Override
    public String executeQuery(DBServer server) throws CmdExecutionException {
            if (server.databaseExists(databaseName)) {
                server.updateCurrentDB(databaseName);
                return "[OK]";
            } else {
                throw new CmdExecutionException("Database: '" + databaseName + "' doesnt exist.");
            }
    }



}



