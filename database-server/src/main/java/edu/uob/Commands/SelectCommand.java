package edu.uob.Commands;

import edu.uob.Conditions.Condition;
import edu.uob.DBCommand;
import edu.uob.DBServer;
import edu.uob.DataStructure.Row;
import edu.uob.DataStructure.Table;
import edu.uob.Exceptions.CmdExecutionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SelectCommand extends DBCommand {

    private ArrayList<String> columnNames;
    private Condition condition;
    private boolean selectAll;
    private final Table table;


    // this selects every column from the table
    public SelectCommand(Table table, boolean selectAll) {
        this.table = table;
        this.selectAll = selectAll;
    }

    // this selects a set of columns from the table
    public SelectCommand(Table table, ArrayList<String> columnNames) {
        this.table = table;
        this.columnNames = columnNames;
    }

    // this selects a specific set of column names
    public SelectCommand(Table table, ArrayList<String> columnNames, Condition condition) {
        this.table = table;
        this.columnNames = columnNames;
        this.condition = condition;
    }


    // this selects every column from the table where the condition is true
    public SelectCommand(Table table, boolean selectAll, Condition condition) {
        this.table = table;
        this.selectAll = selectAll;
        this.condition = condition;
    }



    @Override
    public String executeQuery(DBServer server) throws CmdExecutionException, IOException {
        String tableName = table.getTableName();
        String databaseName = server.getCurrentDatabase();
        if (!server.tableExists(databaseName, tableName)) {
            throw new CmdExecutionException("That table doesn't exist in the database.");
        }
        StringBuilder resultBuilder = new StringBuilder("[OK]\n");

        // Build header row based on whether all columns are selected or specific columns are.
        String header = this.selectAll ? String.join("\t", table.getColumnNames()) : String.join("\t", this.columnNames);
        resultBuilder.append(header).append("\n");
        for (Row row : table.getRows()) { // go through each row to append the data
            if (condition == null || condition.evaluateCondition(row)) {
                List<String> rowData = new ArrayList<>();
                if (this.selectAll) {
                    rowData.addAll(row.getCells()); // if select *, add all cell data from the row
                } else {
                    for (String columnName : this.columnNames) { // else only get thhe data from each specific column
                        //int columnIndex = table.getColumnNames().indexOf(columnName);
                        int columnIndex = table.findColumnIndex(columnName);
                        rowData.add(row.getCellData(columnIndex));
                    }
                }
                resultBuilder.append(String.join("\t", rowData)).append("\n");
            }
        }
        return resultBuilder.toString().trim();
    }



//    @Override
//    public String executeQuery(DBServer server) throws CmdExecutionException, IOException {
//        String tableName = table.getTableName();
//        String databaseName = server.getCurrentDatabase();
//        if (!server.tableExists(databaseName, tableName)) {
//            throw new CmdExecutionException("That table doesn't exist in the database.");
//        }
//        Table displayTable;
//        if (this.selectAll) {
//            displayTable = new Table("display", table.getColumnNames()); // use every column from og table
//        } else {
//            displayTable = new Table("display", this.columnNames);
//        }
//
//        for (Row row : table.getRows()) {
//            if (condition == null || condition.evaluateCondition(row)) {
//                displayTable.insertRow(row);
//            }
//        }
//
//        if (selectAll) {
//            String finalDisplay = displayTable.displayTable();
//            return "[OK]\n" + finalDisplay;
//        } else {
//            String finalDisplay = displayTable.displayPartialTable(this.columnNames);
//            return "[OK]\n" + finalDisplay;
//        }
//    }


}
