package edu.uob.Commands;

import edu.uob.Exceptions.CmdExecutionException;
import edu.uob.DBCommand;
import edu.uob.DBServer;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;

public class CreateDatabase extends DBCommand {
    private final String databaseName;

    public CreateDatabase(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String executeQuery(DBServer server) throws CmdExecutionException {
        if (server.isReservedKeyword(databaseName)) {
            throw new CmdExecutionException("Invalid database name, it cannot be a reserved SQL keyword.");
        }
        try {
            Path path = server.getPathToDatabases().resolve(databaseName);
            if (server.databaseExists(databaseName)) {
                throw new CmdExecutionException("Database " + databaseName + " already exists.");
            } else {
                Files.createDirectories(path);
                return "[OK]";
            }
        } catch (IOException e) {
            throw new CmdExecutionException("Failed to create database due to unknown error.");
        }
    }
}
