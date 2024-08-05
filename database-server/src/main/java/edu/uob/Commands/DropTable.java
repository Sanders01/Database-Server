package edu.uob.Commands;

import edu.uob.DBCommand;
import edu.uob.DBServer;
import edu.uob.Exceptions.CmdExecutionException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DropTable extends DBCommand {

    String tableName;

    public DropTable(String tableName) {
        this.tableName = tableName;
    }

    public String executeQuery(DBServer server) throws CmdExecutionException {
        try {
            Path databasePath = server.getPathToDatabases().resolve(server.getCurrentDatabase());
            Path tablePath = databasePath.resolve(tableName + ".tab");
            Files.delete(tablePath);
        } catch (IOException e) {
            throw new CmdExecutionException("Could not delete table due to unknown error.");
        }
        return "[OK]";
    }

}
