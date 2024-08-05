package edu.uob;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class MetaDataManager {

    private final DBServer server;

    public MetaDataManager(DBServer server) {
        this.server = server;
    }

    public void saveTableMetadata(String databaseName, String tableName, int nextRowID) throws IOException {
        Path databasesPath = server.getPathToDatabases().resolve(databaseName).resolve(tableName + ".meta");
        try (BufferedWriter writer = Files.newBufferedWriter(databasesPath)) {
            writer.write("nextRowID=" + nextRowID);
        }
    }


    public int loadNextRowID(String tableName) throws IOException {
        String databaseName = server.getCurrentDatabase();
        Path databasePath = server.getPathToDatabases().resolve(databaseName);
        Path metadataPath = databasePath.resolve(tableName + ".meta"); // Correctly constructed path for metadata
        if (Files.exists(metadataPath)) {
            List<String> lines = Files.readAllLines(metadataPath);
            for (String line : lines) {
                if (line.startsWith("nextRowID=")) {
                    int nextId = Integer.parseInt(line.split("=")[1]);
                    return nextId;
                }
            }
        } else {
        }
        throw new IOException("Couldn't find path to table's metadata.");
    }



}

