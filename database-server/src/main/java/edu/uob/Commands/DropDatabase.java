package edu.uob.Commands;
import edu.uob.DBCommand;
import edu.uob.DBServer;
import edu.uob.Exceptions.CmdExecutionException;
import java.io.IOException;
import java.nio.file.Path;

public class DropDatabase extends DBCommand {

    private final String databaseName;

    public DropDatabase(String databaseName){
        this.databaseName = databaseName;
    }


    public String executeQuery(DBServer server) throws CmdExecutionException {
        try {
            Path datababasePath = server.getPathToDatabases().resolve(databaseName);
            server.dropDatabase(datababasePath);
        } catch (IOException e) {
            throw new CmdExecutionException("Could not drop the database to to unknown error.");
        }
        return "[OK]";
    }



}
