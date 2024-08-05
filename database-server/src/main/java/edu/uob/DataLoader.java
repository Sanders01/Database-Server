package edu.uob;

import edu.uob.DataStructure.Row;
import edu.uob.DataStructure.Table;
import java.io.IOException;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.Arrays;


public class DataLoader {

    public DBServer server;

    public DataLoader(DBServer server) {
        this.server = server;
    }


    public Table readTableData(String databaseName, String tableName) throws IOException {
        String storageFolderPath = server.getStorageFolderPath();
        Path tablePath = Paths.get(storageFolderPath, databaseName.toLowerCase(), tableName.toLowerCase() + ".tab");
        if (!Files.exists(tablePath)) {
            throw new IOException("Couldn't find a path to the table.");
        }
        Table table;
        try (BufferedReader reader = Files.newBufferedReader(tablePath)) {
            String columnHeaders = reader.readLine();
            if (columnHeaders != null && columnHeaders.contains("\t")) { // check headers exist
                ArrayList<String> columnNames = new ArrayList<>(Arrays.asList(columnHeaders.split("\t")));
                table = new Table(tableName, columnNames);
            } else {
                table = new Table(tableName);
            }
            String rowData;
            while ((rowData = reader.readLine()) != null) {
                ArrayList<String> cells = new ArrayList<>(Arrays.asList(rowData.split("\t")));
                int rowID = Integer.parseInt(cells.get(0));
                cells.remove(0); // Remove row ID since already extracted
                Row row = new Row(rowID, cells);
                table.insertRow(row);
            }
        } catch (IOException e) {
            throw new IOException("Failed to read the table file.");
        }
        return table;
    }



}
