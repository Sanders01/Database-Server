package edu.uob;

import edu.uob.Exceptions.CmdExecutionException;
import edu.uob.Exceptions.ParseException;
import edu.uob.Parsing.Parser;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;

/** This class implements the DB server. */
public class DBServer {

    private static final char END_OF_TRANSMISSION = 4;

    private String storageFolderPath;
    public String getStorageFolderPath() {
        return this.storageFolderPath;
    }

    private String currentDatabase;
    public String getCurrentDatabase() {
        return currentDatabase;
    }


    public String updateCurrentDB(String newDBName) {
        return this.currentDatabase = newDBName;
    }

    public static void main(String args[]) throws IOException {
        DBServer server = new DBServer();
        server.blockingListenOn(8888);
    }

    /**
    * KEEP this signature otherwise we won't be able to mark your submission correctly.
    */
    public DBServer() {
        storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        try {
            // Create the database storage folder if it doesn't already exist !
            Files.createDirectories(Paths.get(storageFolderPath));
        } catch(IOException ioe) {
            System.out.println("Can't seem to create database storage folder " + storageFolderPath);
        }
    }

    public Path getPathToDatabases() {
        return Path.of(storageFolderPath);
    }

    /**
    * KEEP this signature (i.e. {@code edu.uob.DBServer.handleCommand(String)}) otherwise we won't be
    * able to mark your submission correctly.
    *
    * <p>This method handles all incoming DB commands and carries out the required actions.
    */
    public String handleCommand(String command) {
        try {
            Tokeniser tokeniser = new Tokeniser();
            String[] initialTokens = tokeniser.tokenise(command);
            ArrayList<String> consolidatedTokens = tokeniser.connectStringLiterals(new ArrayList<>(Arrays.asList(initialTokens)));
            ArrayList<String> finalTokens = tokeniser.combineEqualityTokens(consolidatedTokens);
            String[] tokens = finalTokens.toArray(new String[0]);
            Parser parser = new Parser(this, tokens);
            DBCommand query = parser.parseTokens(tokens);
            return query.executeQuery(this);
        } catch (ParseException e) {
            return "[ERROR] " + e.getMessage();
        } catch (CmdExecutionException e) {
            return "[ERROR] " + e.getMessage();
        } catch (IOException e) {
            return "[ERROR] The program encountered an issue reading from the file system.";
        } catch (Exception e) {
            return "[ERROR] Unexpected error!";
        }
    }

    private static final String[] reservedKeywords = { "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "DELETE",
            "CREATE", "TABLE", "DATABASE", "USE", "DROP", "AND", "OR", "LIKE",
            "TRUE", "FALSE", "NOT", "JOIN", "ON", "UPDATE", "SET", "ALTER" };


    public boolean isReservedKeyword(String token) {
        String standadisedToken = token.toUpperCase();

        for (String keyword : reservedKeywords) {
            if (keyword.equals(standadisedToken)) {
                return true;
            }
        }
        return false;
    }


    public boolean databaseExists(String databaseName) {
        Path databasePath = Paths.get(storageFolderPath, databaseName.toLowerCase());
        return Files.isDirectory(databasePath);
    }


    public void dropDatabase(Path databasePath) throws IOException {
        Files.walkFileTree(databasePath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }



    public boolean tableExists(String databaseName, String tableName) throws CmdExecutionException {
        if (!databaseExists(databaseName)) {
            throw new CmdExecutionException("The database name provided does not exist.");
        }
        Path tablePath = Paths.get(storageFolderPath, databaseName.toLowerCase(), tableName.toLowerCase() + ".tab");

        return Files.exists(tablePath);
    }



    //  === Methods below handle networking aspects of the project - you will not need to change these ! ===

    public void blockingListenOn(int portNumber) throws IOException {
        try (ServerSocket s = new ServerSocket(portNumber)) {
            System.out.println("Server listening on port " + portNumber);
            while (!Thread.interrupted()) {
                try {
                    blockingHandleConnection(s);
                } catch (IOException e) {
                    System.err.println("Server encountered a non-fatal IO error:");
                    e.printStackTrace();
                    System.err.println("Continuing...");
                }
            }
        }
    }

    private void blockingHandleConnection(ServerSocket serverSocket) throws IOException {
        try (Socket s = serverSocket.accept();
        BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()))) {

            System.out.println("Connection established: " + serverSocket.getInetAddress());
            while (!Thread.interrupted()) {
                String incomingCommand = reader.readLine();
                System.out.println("Received message: " + incomingCommand);
                String result = handleCommand(incomingCommand);
                writer.write(result);
                writer.write("\n" + END_OF_TRANSMISSION + "\n");
                writer.flush();
            }
        }
    }


}

