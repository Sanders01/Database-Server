package edu.uob.DataStructure;

import edu.uob.Exceptions.CmdExecutionException;
import java.io.IOException;
import java.util.*;
import java.io.BufferedWriter;
import java.io.FileWriter;


public class Table {

    private final String tableName;

    public final String getTableName() {
        return tableName;
    }

    public final ArrayList<String> columnNames;

    public final ArrayList<String> getColumnNames() {
        return new ArrayList<>(columnNames);
    }

    public void addAttribute(String newAttribute) throws CmdExecutionException{
        if (!this.columnNames.contains(newAttribute)) {
            this.columnNames.add(newAttribute);
        } else {
            throw new CmdExecutionException("Column name '" + newAttribute + "' already exists.");
        }
    }

    private final ArrayList<Row> rows = new ArrayList<>();

    public ArrayList<Row> getRows() {
        return this.rows;
    }

    private int nextRowID = 1;


    public int getNextRowID() {
        return nextRowID;
    }




    public Table(String tableName, ArrayList<String> columnNames) {
        this.tableName = tableName;
        this.columnNames = new ArrayList<>();
        this.columnNames.add("id"); // Ensure "id" is always first
        for (String columnName : columnNames) {
            if (!"id".equalsIgnoreCase(columnName)) { // Skip "id" in the input list to avoid duplication
                this.columnNames.add(columnName);
            }
        }
    }


    public Table(String tableName) {
        this.tableName = tableName;
        this.columnNames = new ArrayList<>();
        this.columnNames.add("id");
    }


    public int findColumnIndex(String columnName) throws CmdExecutionException {
        String colNameTrim = columnName.trim();
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(colNameTrim)) {
                return i;
            }
        }
        throw new CmdExecutionException("That column name doesnt exist in the table.");
    }



    public boolean attributeExists(String columnName) {
        return columnNames.contains(columnName);
    }



    public void initialiseEmptyColumn() {
        for (Row row : rows) {
            row.getCells().add(" ");
        }
    }


    public void removeColumnHeader(int columnIndex) {
        columnNames.remove(columnIndex);
    }


    public void removeColumnFromRows(int index) {
        for (Row row : rows) {
            ArrayList<String> cells = row.getCells();
            if (index < cells.size()) {
                cells.remove(index);
            }
        }
    }


    public void insertRow(Row newRow) {
        rows.add(newRow);
        nextRowID++;
    }



    public void saveToFile(String path) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
            writer.write(String.join("\t", columnNames));
            writer.newLine();
            for (Row row : rows) {
                String rowString = String.join("\t", row.getCells());
                writer.write(rowString);
                writer.newLine();
            }
        }
    }


//    public String displayTable() {
//        StringBuilder builder = new StringBuilder();
//
//        // Calculate the maximum width for each column
//        ArrayList<Integer> columnWidths = new ArrayList<>(columnNames.size());
//        for (String columnName : columnNames) {
//            columnWidths.add(columnName.length());
//        }
//
//        for (Row row : rows) {
//            ArrayList<String> cells = row.getCells();
//            for (int i = 0; i < cells.size(); i++) {
//                if (cells.get(i).length() > columnWidths.get(i)) {
//                    columnWidths.set(i, cells.get(i).length());
//                }
//            }
//        }
//
//        // Create the header row
//        for (int i = 0; i < columnNames.size(); i++) {
//            String header = columnNames.get(i);
//            builder.append(String.format("%-" + columnWidths.get(i) + "s\t", header));
//        }
//        builder.append("\n");
//
//        // Add a divider line
//        for (int width : columnWidths) {
//            builder.append("-".repeat(width)).append("\t");
//        }
//        builder.append("\n");
//
//        for (Row row : rows) {
//            ArrayList<String> cells = row.getCells();
//            for (int i = 0; i < cells.size(); i++) {
//                String cellData = cells.get(i);
//                builder.append(String.format("%-" + columnWidths.get(i) + "s\t", cellData));
//            }
//            builder.append("\n");
//        }
//
//        System.out.println(builder.toString());
//        return builder.toString().trim(); // Trims the last newline character
//    }


    public String displayTable() {
        StringBuilder builder = new StringBuilder();
        builder.append(String.join("\t", columnNames)).append("\n");
        for (Row row : rows) {
            ArrayList<String> cells = row.getCells();
            for (Object cell : cells) {
                builder.append(cell).append("\t");
            }
            builder.append("\n");
        }
        return builder.toString().trim(); // gets rid of newline char
    }




}
