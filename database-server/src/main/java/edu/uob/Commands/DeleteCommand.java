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

public class DeleteCommand extends DBCommand {

    private final Table table;
    private final Condition condition;


    public DeleteCommand(Table table, Condition condition) {
        this.table = table;
        this.condition = condition;
    }

    public String executeQuery(DBServer server) throws CmdExecutionException, IOException {
        String tableName = table.getTableName();
        String databaseName = server.getCurrentDatabase();
        Path databasePath = Paths.get(server.getPathToDatabases().toString(), databaseName, tableName + ".tab");

        if (!server.tableExists(databaseName, tableName)) {
            throw new CmdExecutionException("That table doesn't exist in the current database.");
        }

        Table newTable = new Table(tableName, table.getColumnNames());
        for (Row row : table.getRows()) {
            if (!condition.evaluateCondition(row)) {
                newTable.insertRow(row);
            }
        }

        newTable.saveToFile(databasePath.toString());

        return "[OK]";
    }
}