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

public class AlterDrop extends DBCommand {

    String columnName;
    String tableName;

    public AlterDrop(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }


    public String executeQuery(DBServer server) throws CmdExecutionException {
        if (columnName.equalsIgnoreCase("id")) {
            throw new CmdExecutionException("You cannot remove the 'id' column from a table.");
        }
        try {
            String databaseName = server.getCurrentDatabase();
            Path databasePath = Paths.get(server.getPathToDatabases().toString(), databaseName, tableName + ".tab");
            DataLoader loader = new DataLoader(server);
            Table table = loader.readTableData(databaseName, tableName);
            ArrayList<String> columnNames = table.getColumnNames();
            boolean colExists = columnNames.contains(columnName);
            if (!table.attributeExists(columnName)) {
                throw new CmdExecutionException("The attribute you are trying to drop doesnt exist.");
            }
            if (colExists) {
                int index = table.findColumnIndex(columnName);
                System.out.print(index);
                if (index == -1) {
                    throw new CmdExecutionException("That column name doesnt exist the this table.");
                }
                table.removeColumnFromRows(index);
                table.removeColumnHeader(index);
                table.saveToFile(databasePath.toString());
            }
        } catch (IOException e) {
            throw new CmdExecutionException("Unexpected error during ALTER (DROP) command execution.");
        }
        return "[OK]";
    }
}


