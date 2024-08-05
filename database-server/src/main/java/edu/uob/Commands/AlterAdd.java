package edu.uob.Commands;

import edu.uob.DBCommand;
import edu.uob.DBServer;
import edu.uob.DataLoader;
import edu.uob.DataStructure.Table;
import edu.uob.Exceptions.CmdExecutionException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class AlterAdd extends DBCommand {

    String columnName;
    String tableName;

    public AlterAdd(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }


    public String executeQuery(DBServer server) throws CmdExecutionException {
        String databaseName = server.getCurrentDatabase();
        if (databaseName == null || tableName == null) {
            throw new CmdExecutionException("Database name or table name is null.");
        }
        if (columnName.equalsIgnoreCase("id")) {
            throw new CmdExecutionException("You cannot add another 'id' column to a table.");
        }
        Path databasePath = Paths.get(server.getPathToDatabases().toString(), databaseName, tableName + ".tab");
        DataLoader loader = new DataLoader(server);
        try {
            Table table = loader.readTableData(databaseName, tableName);
            ArrayList<String> columnNames = table.getColumnNames();

            if (server.isReservedKeyword(columnName)) {
                throw new CmdExecutionException("Invalid attribute name, it is a reserved keyword.");
            }
            boolean colExists = columnNames.stream().anyMatch(name -> name.trim().equalsIgnoreCase(columnName.trim()));
            if (!colExists) {
                table.addAttribute(columnName);
                table.initialiseEmptyColumn();
                table.saveToFile(databasePath.toString());
            } else {
                throw new CmdExecutionException("You cannot have multiple columns with the same name.");
            }
        } catch (IOException e) {
            throw new CmdExecutionException("Unexpected error during 'ALTER ADD' .");
        }
        return "[OK]";
    }
}

