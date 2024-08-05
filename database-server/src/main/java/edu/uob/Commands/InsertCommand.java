package edu.uob.Commands;

import edu.uob.DBCommand;
import edu.uob.DBServer;
import edu.uob.DataLoader;
import edu.uob.DataStructure.Row;
import edu.uob.DataStructure.Table;
import edu.uob.Exceptions.CmdExecutionException;
import edu.uob.MetaDataManager;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class InsertCommand extends DBCommand {

    private final String tableName;
    private final ArrayList<String> newRowValues;

    public InsertCommand (String tableName, ArrayList<String> newRowValues) {
        this.tableName = tableName;
        this.newRowValues = newRowValues;
    }

    @Override
    public String executeQuery(DBServer server) throws CmdExecutionException, IOException {
        if (newRowValues.isEmpty()) {
            throw new CmdExecutionException("No values were provided for insertion.");
        }
        try {
            String databaseName = server.getCurrentDatabase();
            if (databaseName == null || databaseName.isEmpty()) {
                throw new CmdExecutionException("No database is in use.");
            }
            if (!server.tableExists(databaseName, tableName)) {
                throw new CmdExecutionException("Table does not exist.");
            }
            Path databasePath = Paths.get(server.getPathToDatabases().toString(), databaseName, tableName + ".tab");
            DataLoader loader = new DataLoader(server);
            MetaDataManager metaDataManager = new MetaDataManager(server);
            Table table = loader.readTableData(databaseName, tableName);
            int tableAttributes = table.getColumnNames().size() - 1; // take out the id column
            int newRowSize = newRowValues.size();
            if (newRowSize > tableAttributes) {
                throw new CmdExecutionException("You cannot insert more attributes than there are column headers.");
            }
            if (newRowSize < 1) {
                throw new CmdExecutionException("No values were provided for insertion.");
            }
            int nextRowId = metaDataManager.loadNextRowID(tableName);
            Row newRow = new Row(nextRowId, newRowValues);
            table.insertRow(newRow);
            metaDataManager.saveTableMetadata(databaseName, tableName, nextRowId + 1);
            table.saveToFile(databasePath.toString());
            return "[OK]";
        } catch (IOException e) {
            throw new CmdExecutionException("Unexpected error during INSERT command execution.");
        }
    }
}
