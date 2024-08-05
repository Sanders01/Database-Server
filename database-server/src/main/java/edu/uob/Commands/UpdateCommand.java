package edu.uob.Commands;

import edu.uob.Conditions.Condition;
import edu.uob.DBCommand;
import edu.uob.DBServer;
import edu.uob.DataStructure.Row;
import edu.uob.DataStructure.Table;
import edu.uob.Exceptions.CmdExecutionException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

public class UpdateCommand extends DBCommand {

    private final Table table;
    private final HashMap<String, String> updates;
    private final Condition condition;

    public UpdateCommand(Table table, HashMap<String, String> updates, Condition condition) {
        this.table = table;
        this.updates = updates;
        this.condition = condition;
    }

    @Override
    public String executeQuery(DBServer server) throws CmdExecutionException, IOException {
        String tableName = table.getTableName();
        String databaseName = server.getCurrentDatabase();
        Path databasePath = Paths.get(server.getPathToDatabases().toString(), databaseName, tableName + ".tab");
        if (!server.tableExists(databaseName, tableName)) {
            throw new CmdExecutionException("That table doesn't exist in the current database.");
        }
        if (updates.containsKey("id")) {
            throw new CmdExecutionException("You cannot alter the values in the 'id' column.");
        }

        for (Row row : table.getRows()) {
            if (condition.evaluateCondition(row)) {
                for (String columnName : updates.keySet()) {
                    int columnIndex = table.findColumnIndex(columnName);
                    if (columnIndex != -1) {
                        row.getCells().set(columnIndex, updates.get(columnName));
                    } else {
                        throw new CmdExecutionException("Column name '" + columnName + "' does not exist in the table.");
                    }
                }
            }
        }

        table.saveToFile(databasePath.toString());
        return "[OK]";
    }
}

