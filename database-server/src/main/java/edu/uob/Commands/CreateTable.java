package edu.uob.Commands;

import edu.uob.DataStructure.Table;
import edu.uob.Exceptions.CmdExecutionException;
import edu.uob.DBCommand;
import edu.uob.DBServer;
import edu.uob.MetaDataManager;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;

public class CreateTable extends DBCommand {

    private final String tableName;
    private final ArrayList<String> columnNames;

    public CreateTable(String tableName, ArrayList<String> columnsNames) throws CmdExecutionException {

        this.tableName = tableName.toLowerCase();
        this.columnNames = columnsNames;
        HashSet<String> uniqueColumnCheck = new HashSet<>();

        for (String columnName : columnNames) {
            if (!uniqueColumnCheck.add(columnName.toLowerCase())) { // check if there is a duplicate
                throw new CmdExecutionException("Duplicate column name: '" + columnName + "' in 'CREATE TABLE' command.");
            }
        }
    }

    @Override
    public String executeQuery(DBServer server) throws CmdExecutionException {
        if (server.getCurrentDatabase() == null) {
            throw new CmdExecutionException("No database is in use.");
        }
        if (server.tableExists(server.getCurrentDatabase(), tableName.toLowerCase())) {
            throw new CmdExecutionException("The table you are attempting to create already exists.");
        }
        if (server.isReservedKeyword(tableName)) {
            throw new CmdExecutionException("That table name is a reserved SQL keyword.");
        }
        for (String columnName : columnNames) {
            if (server.isReservedKeyword(columnName)) {
                throw new CmdExecutionException("The column name '" + columnName + "' is a reserved SQL keyword.");
            }
            if (columnName.equalsIgnoreCase("id")) {
                throw new CmdExecutionException("You cannot create a column name with the 'id' header.");
            }
        }
        Path databasePath = server.getPathToDatabases().resolve(server.getCurrentDatabase());
        Path tablePath = databasePath.resolve(tableName.toLowerCase() + ".tab");
        Table newTable;
        if (columnNames.isEmpty()) {
            newTable = new Table(tableName);
        } else {
            newTable = new Table(tableName, columnNames);
        }
        try {
            MetaDataManager manager = new MetaDataManager(server);
            int initialRowId = newTable.getNextRowID();
            manager.saveTableMetadata(databasePath.toString(), tableName, initialRowId);
            newTable.saveToFile(tablePath.toString());
            return "[OK]";
        } catch (IOException e) {
            throw new CmdExecutionException("Failed to create table due to an IO error.");
        }
    }
}

