package edu.uob.Commands;

import edu.uob.DBCommand;
import edu.uob.DBServer;
import edu.uob.DataLoader;
import edu.uob.DataStructure.Row;
import edu.uob.DataStructure.Table;
import edu.uob.Exceptions.CmdExecutionException;
import java.io.IOException;
import java.util.ArrayList;

public class JoinCommand extends DBCommand {

    String firstTableName;
    String secondTableName;
    String firstAttribute;
    String secondAttribute;

    public JoinCommand(String firstTable, String secondTable, String firstAttrribute, String secondAttribute) {
        this.firstTableName = firstTable;
        this.secondTableName = secondTable;
        this.firstAttribute = firstAttrribute;
        this.secondAttribute = secondAttribute;
    }

    private Table buildJointTable(Table tableOne, Table tableTwo) throws CmdExecutionException {
        ArrayList<String> jointColumnNames = new ArrayList<>();
        jointColumnNames.add("id");
        tableOne.getColumnNames().stream()
                .filter(colName -> !colName.equals("id") && !colName.equals(firstAttribute))
                .forEach(colName -> jointColumnNames.add(tableOne.getTableName() + "." + colName));

        tableTwo.getColumnNames().stream()
                .filter(colName -> !colName.equals("id") && !colName.equals(secondAttribute))
                .forEach(colName -> jointColumnNames.add(tableTwo.getTableName() + "." + colName));

        Table jointTable = new Table("joinedTable", jointColumnNames);
        int newRowId = 1; // Start with an ID of 1 for the first row of the joint table
        // Finding matching rows based on the join attributes and creating new rows in the joint table
        for (Row rowOne : tableOne.getRows()) {
            for (Row rowTwo : tableTwo.getRows()) {
                String valOne = rowOne.getCellData(firstAttribute, tableOne);
                String valTwo = rowTwo.getCellData(secondAttribute, tableTwo);

                if (valOne.equals(valTwo)) {
                    ArrayList<String> newRowCells = new ArrayList<>();
                    newRowCells.add(String.valueOf(newRowId++)); // Adding new row ID
                    // Adding cell data from both tables to the new row, excluding the join attribute columns
                    addRowDataExcludingAttributes(rowOne, tableOne, firstAttribute, newRowCells);
                    addRowDataExcludingAttributes(rowTwo, tableTwo, secondAttribute, newRowCells);
                    jointTable.insertRow(new Row(newRowId, newRowCells));
                }
            }
        }
        return jointTable;
    }

    private void addRowDataExcludingAttributes(Row sourceRow, Table sourceTable, String excludeAttribute, ArrayList<String> targetCells) throws CmdExecutionException {
        for (String colName : sourceTable.getColumnNames()) {
            if (!colName.equals("id") && !colName.equals(excludeAttribute)) {
                String cellData = sourceRow.getCellData(colName, sourceTable);
                targetCells.add(cellData);
            }
        }
    }

    public String executeQuery(DBServer server) throws CmdExecutionException, IOException {
        String databaseName = server.getCurrentDatabase();
        DataLoader loader = new DataLoader(server);
        Table tableOne = loader.readTableData(databaseName, this.firstTableName);
        Table tableTwo = loader.readTableData(databaseName, this.secondTableName);

        Table joinedTable = buildJointTable(tableOne, tableTwo);
        String joinedDisplay = joinedTable.displayTable();
        return "[OK]\n" + joinedDisplay;
    }

}

