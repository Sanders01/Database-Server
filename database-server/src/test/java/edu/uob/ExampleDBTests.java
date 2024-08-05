package edu.uob;

import edu.uob.Commands.CreateDatabase;
import edu.uob.Commands.UseDatabase;
import edu.uob.Exceptions.ParseException;
import edu.uob.Parsing.Parser;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;

public class ExampleDBTests {

    private DBServer server;

    // Create a new server _before_ every @Test
    @BeforeEach
    public void setup() {
        server = new DBServer();
    }

    // Random name generator - useful for testing "bare earth" queries (i.e. where tables don't previously exist)
    private String generateRandomName() {
        String randomName = "";
        for (int i = 0; i < 10; i++) randomName += (char) (97 + (Math.random() * 25.0));
        return randomName;
    }

    private String sendCommandToServer(String command) {
        // Try to send a command to the server - this call will timeout if it takes too long (in case the server enters an infinite loop)
        return assertTimeoutPreemptively(Duration.ofMillis(1000), () -> {
                    return server.handleCommand(command);
                },
                "Server took too long to respond (probably stuck in an infinite loop)");
    }

    // A basic test that creates a database, creates a table, inserts some test data, then queries it.
    // It then checks the response to see that a couple of the entries in the table are returned as expected
    @Test
    public void testBasicCreateAndQuery() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Sion', 55, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Rob', 35, FALSE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Chris', 20, FALSE);");
        String response = sendCommandToServer("SELECT * FROM marks;");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("Simon"), "An attempt was made to add Simon to the table, but they were not returned by SELECT *");
        assertTrue(response.contains("Chris"), "An attempt was made to add Chris to the table, but they were not returned by SELECT *");
    }

    // A test to make sure that querying returns a valid ID (this test also implicitly checks the "==" condition)
    // (these IDs are used to create relations between tables, so it is essential that suitable IDs are being generated and returned !)
    @Test
    public void testQueryID() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        String response = sendCommandToServer("SELECT id FROM marks WHERE name == 'Simon';");
        // Convert multi-lined responses into just a single line
        String singleLine = response.replace("\n", " ").trim();
        // Split the line on the space character
        String[] tokens = singleLine.split(" ");
        // Check that the very last token is a number (which should be the ID of the entry)
        String lastToken = tokens[tokens.length - 1];
        try {
            Integer.parseInt(lastToken);
        } catch (NumberFormatException nfe) {
            fail("The last token returned by `SELECT id FROM marks WHERE name == 'Simon';` should have been an integer ID, but was " + lastToken);
        }
    }

    // A test to make sure that databases can be reopened after server restart
    @Test
    public void testTablePersistsAfterRestart() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        // Create a new server object
        server = new DBServer();
        sendCommandToServer("USE " + randomName + ";");
        String response = sendCommandToServer("SELECT * FROM marks;");
        assertTrue(response.contains("Simon"), "Simon was added to a table and the server restarted - but Simon was not returned by SELECT *");
    }

    // Test to make sure that the [ERROR] tag is returned in the case of an error (and NOT the [OK] tag)
    @Test
    public void testForErrorTag() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        String response = sendCommandToServer("SELECT * FROM libraryfines;");
        assertTrue(response.contains("[ERROR]"), "An attempt was made to access a non-existent table, however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An attempt was made to access a non-existent table, however an [OK] tag was returned");
    }


    @Test
    public void testTokeniser() {
        Tokeniser testTokeniser = new Tokeniser();
        String query = testTokeniser.query = "CREATE TABLE students (id INT, name VARCHAR);";
        testTokeniser.setup();
        String[] tokens = testTokeniser.tokenise(query);
        String[] expectedTokens = {"CREATE", "TABLE", "students", "(", "id", "INT", ",", "name", "VARCHAR", ")", ";"};
        for (int i = 0; i < expectedTokens.length; i++) {
            assertTrue(expectedTokens[i].equals(tokens[i]));
        }
    }

    @Test
    public void testTokeniserWithComparisons() {
        Tokeniser tokeniser = new Tokeniser();
        String query = tokeniser.query = "SELECT * FROM results WHERE (pass == false) AND mark > 35);";
    }

// ======================================== Parsing Tests ======================================= //

    // ==== Parse USE Tests ==== //
    @Test
    public void testParseUseValid() {
        String[] tokens = {"USE", "testDB", ";"};
        DBServer server = null;
        Parser parser = new Parser(server, tokens);
        assertDoesNotThrow(() -> {
            DBCommand command = parser.parseUse();
            assertTrue(command instanceof UseDatabase);
            UseDatabase useCommand = (UseDatabase) command;
            assertEquals("testdb", useCommand.getDatabaseName());
        });
    }



    @Test
    public void testParseUseValid2() {
        DBServer server = null;
        String[] tokens = {"use", "testDB", ";"};
        Parser parser = new Parser(server, tokens);
        assertDoesNotThrow(() -> {
            DBCommand command = parser.parseUse();
            assertTrue(command instanceof UseDatabase);
            UseDatabase useCommand = (UseDatabase) command;
            assertEquals("testdb", useCommand.getDatabaseName());
        });
    }


    @Test
    public void testParseUseEmptyDatabaseName() {
        DBServer server = null;
        String[] tokens = {"USE"};
        Parser parser = new Parser(server, tokens);
        assertThrows(ParseException.class, () -> parser.parseUse());
    }

    @Test
    public void testParseUseExtraTokens() {
        DBServer server = null;
        String[] tokens = {"USE", "testDB", "exToken", ";"};
        Parser parser = new Parser(server, tokens);
        assertThrows(ParseException.class, () -> parser.parseUse());
    }

    // ==== Parse CREATE Tests ==== //
    @Test
    public void testParseCreateDatabaseValid() {
        DBServer server = null;
        String[] tokens = {"CREATE", "DATABASE", "testDB"};
        Parser parser = new Parser(server, tokens);
        assertDoesNotThrow(() -> {
            DBCommand command = parser.parseCreate();
            assertTrue(command instanceof CreateDatabase);
            CreateDatabase createCommand = (CreateDatabase) command;
            assertEquals("testdb", createCommand.getDatabaseName());
        });
    }

    @Test
    public void createTableNoClosingParenthesis() {
        DBServer server = null;
        String[] tokens = {"CREATE", "TABLE", "users", "(", "id", "INT", ";"};
        Parser parser = new Parser(server, tokens);
        assertThrows(ParseException.class, () -> parser.parseCreate());
    }

    @Test
    public void createTableDuplicateColumnNames() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        String query = sendCommandToServer("CREATE TABLE people (name, name);");
        assertFalse(query.contains("[OK]"));
        assertTrue(query.contains("[ERROR]"));
    }


    // ========================================  INSERT  ======================================== //

    @Test
    public void testInsertWithOneColumn() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        String query = sendCommandToServer("CREATE   TABle people  (name);");
        assertTrue(query.contains("[OK]"));
        assertFalse(query.contains("[ERROR]"));
        sendCommandToServer("INSERT INTO people VALUES ('Tom');");
        String select = sendCommandToServer("SELECT * FROM people;");
        assertTrue(select.contains("name"));
        assertTrue(select.contains("'Tom'"));

    }

    @Test
    public void insertTooFewValues() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        String query = sendCommandToServer("CREATE TABLE people  (name);");
        assertTrue(query.contains("[OK]"));
        assertFalse(query.contains("[ERROR]"));
        String insertQuery = sendCommandToServer("INSERT INTO people VALUES ( , , , );");
        assertFalse(insertQuery.contains("[OK]"));
        assertTrue(insertQuery.contains("[ERROR]"));
    }

    @Test
    public void insertMoreValuesThanColumns() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        String query = sendCommandToServer("CREATE TABLE people (name, age);");
        assertTrue(query.contains("[OK]"));
        assertFalse(query.contains("[ERROR]"));
         String insertQuery = sendCommandToServer("INSERT INTO people VALUES ('Tom', 26, 230303);");
        assertFalse(insertQuery.contains("[OK]"));
        assertTrue(insertQuery.contains("[ERROR]"));
    }

    @Test
    public void testSimilarNumbersMultiAttribute() {
        String randDbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randDbName + ";");
        sendCommandToServer("USE " + randDbName + ";");
        sendCommandToServer("CREATE TABLE numbers (number, isPositive);");
        sendCommandToServer("INSERT INTO numbers VALUES (10.0, TRUE);");
        sendCommandToServer("INSERT INTO numbers VALUES (10.1, TRUE);");
        sendCommandToServer("INSERT INTO numbers VALUES (23, TRUE);");
        String numTest = sendCommandToServer("SELECT * FROM numbers WHERE number == 10;");
        assertTrue(numTest.contains("[OK]"));
        assertTrue(numTest.contains("10.0"));
        assertFalse(numTest.contains("10.1"));
        String noResult = sendCommandToServer("SELECT * FROM numbers WHERE number == 10.5;");
        assertTrue(noResult.contains("[OK]"));
        assertFalse(noResult.contains("10.1"));
        assertFalse(noResult.contains("TRUE"));
    }

    @Test
    public void createTableOneAttributeAndInsertData() {
        String randDbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randDbName + ";");
        sendCommandToServer("USE " + randDbName + ";");
        sendCommandToServer("CREATE TABLE numbers (number);");
        sendCommandToServer("INSERT INTO numbers VALUES (10);");
        String tableState = sendCommandToServer("SELECT * FROM numbers;");
        assertTrue(tableState.contains("number"));
        assertTrue(tableState.contains("10"));
    }


    @Test
    public void testTableNameLowercaseButAttriNamesCaseSensitive() {
        String randDbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randDbName + ";");
        sendCommandToServer("USE " + randDbName + ";");
        sendCommandToServer("CREATE TABLE NUMBERS (number, NUMBERS, CamelCased);");
        String query = sendCommandToServer("SELECT * FROM numbers;");
        assertTrue(query.contains("NUMBERS"));
        assertTrue(query.contains("number"));
        assertTrue(query.contains("CamelCased"));
        assertFalse(query.contains("camelcased"));
        assertFalse(query.contains("numbers"));
    }



    // ========================================  SELECT  ======================================== //

    @Test
    public void selectSpecificColumnsWithCondition() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE people (name, age, occupation);");
        sendCommandToServer("INSERT INTO people VALUES ('tom', 26, 'lifeguard');");
        sendCommandToServer("INSERT INTO people VALUES ('saf', 23, 'photographer');");
        sendCommandToServer("INSERT INTO people VALUES ('mike', 62, 'teacher');");
        String select = sendCommandToServer("SELECT occupation FROM people;");
        assertTrue(select.contains("[OK]"));
        assertTrue(select.contains("'lifeguard'"));

    }

    @Test
    public void selectTwoColumnsNoCondition() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE people (name, age, occupation);");
        sendCommandToServer("INSERT INTO people VALUES ('tom', 26, 'lifeguard');");
        sendCommandToServer("INSERT INTO people VALUES ('saf', 23, 'photographer');");
        sendCommandToServer("INSERT INTO people VALUES ('mike', 62, 'teacher');");
        String select = sendCommandToServer("SELECT occupation, name FROM people;");
        assertTrue(select.contains("[OK]"));
        assertTrue(select.contains("'lifeguard'"));
        assertTrue(select.contains("'tom'"));
        assertFalse(select.contains("id"));
        assertFalse(select.contains("26"));
    }

    @Test
    public void selectWrongattributeName() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE people (name, age, occupation);");
        sendCommandToServer("INSERT INTO people VALUES ('tom', 26, 'lifeguard');");
        sendCommandToServer("INSERT INTO people VALUES ('saf', 23, 'photographer');");
        sendCommandToServer("INSERT INTO people VALUES ('mike', 62, 'teacher');");
        String select = sendCommandToServer("SELECT rat, name FROM people;");
        assertFalse(select.contains("[OK]"));
        assertTrue(select.contains("[ERROR] That column name doesnt exist in the table."));
    }



    @Test
    public void selectTwoColumnsLikeCondition() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE people (name, age, occupation);");
        sendCommandToServer("INSERT INTO people VALUES ('tom', 26, 'lifeguard');");
        sendCommandToServer("INSERT INTO people VALUES ('saf', 23, 'photographer');");
        sendCommandToServer("INSERT INTO people VALUES ('mike', 62, 'teacher');");
        String select = sendCommandToServer("SELECT occupation, name FROM people WHERE name LIKE m;");
        assertTrue(select.contains("[OK]"));
        assertTrue(select.contains("'lifeguard'"));
        assertTrue(select.contains("'tom'"));
        assertFalse(select.contains("id"));
        assertFalse(select.contains("26"));
        assertFalse(select.contains("'saf'"));
        assertFalse(select.contains("62"));
        assertTrue(select.contains("'teacher'"));
        assertTrue(select.contains("'mike'"));
    }

    @Test
    public void selectOneColumnsGreaterThanCondition() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE people (name, age, occupation);");
        sendCommandToServer("INSERT INTO people VALUES ('tom', 26, 'lifeguard');");
        sendCommandToServer("INSERT INTO people VALUES ('saf', 23, 'photographer');");
        sendCommandToServer("INSERT INTO people VALUES ('mike', 62, 'teacher');");
        String select = sendCommandToServer("SELECT name FROM people WHERE (age > 20 AND age < 30);");
        assertTrue(select.contains("[OK]"));
        assertTrue(select.contains("'tom'"));
        assertFalse(select.contains("id"));
        assertFalse(select.contains("'mike'"));
        assertFalse(select.contains("62"));
        assertTrue(select.contains("'saf'"));
    }



    @Test
    public void testSelectNoMatches() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE people (name, age);");
        sendCommandToServer("INSERT INTO testTable VALUES (1, 'John Doe', 30);");
        String result = sendCommandToServer("UPDATE testTable SET nonExistingColumn = 'value' WHERE id = 1;");
        assertFalse(result.contains("[OK]"));
        assertTrue(result.contains("[ERROR]"));

    }


    @Test
    public void testSelectEmptyTable() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE people (name, age);");
        String select = sendCommandToServer("SELECT * FROM people;");
        assertTrue(select.contains("name"));
        assertTrue(select.contains("age"));

    }


    // ========================================  UPDATE  ======================================== //

    @Test
    public void testUpdateWithNonExistingColumn() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE people (name, age);");
        sendCommandToServer("INSERT INTO people VALUES ('Tom', 26);");
        sendCommandToServer("INSERT INTO people VALUES ('Saf', 23);");
        String select = sendCommandToServer("SELECT * FROM people WHERE name LIKE 'Casp';");
        assertFalse(select.contains("24"));
        assertFalse(select.contains("Tom"));
        assertTrue(select.contains("name"));
        assertTrue(select.contains("age"));
        assertTrue(select.contains("id"));
    }

    @Test
    public void testUpdateInvalidComparatorSymbol() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE people (name, age);");
        sendCommandToServer("INSERT INTO people VALUES ('Tom', 26);");
        sendCommandToServer("INSERT INTO people VALUES ('Saf', 23);");
        String select = sendCommandToServer("SELECT * FROM people WHERE name >> 26;");
        assertTrue(select.contains("[ERROR]"));
        assertFalse(select.contains("[OK]"));
    }

    @Test
    public void testUpdateInvalidComparatorSymbol2() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE people (name, age);");
        sendCommandToServer("INSERT INTO people VALUES ('Tom', 26);");
        sendCommandToServer("INSERT INTO people VALUES ('Saf', 23);");
        String select = sendCommandToServer("SELECT * FROM people WHERE name ! 26;");
        assertTrue(select.contains("[ERROR] Invalid comparator symbol"));
        assertFalse(select.contains("[OK]"));
    }

    @Test
    public void testUpdateInvalidComparatorSymbol3() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE people (name, age);");
        sendCommandToServer("INSERT INTO people VALUES ('Tom', 26);");
        sendCommandToServer("INSERT INTO people VALUES ('Saf', 23);");
        String select = sendCommandToServer("SELECT * FROM people WHERE name =< 26;");
        assertTrue(select.contains("[ERROR] Invalid comparator symbol"));
        assertFalse(select.contains("[OK]"));
    }



    @Test
        public void testSetNoSpacesBetweenAttributeAndValue() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE people (name, age);");
        sendCommandToServer("INSERT INTO people VALUES ('Tom', 26);");
        sendCommandToServer("INSERT INTO people VALUES ('Saf', 23);");
        String query = sendCommandToServer("UPDATE people SET age=24 WHERE name LIKE 'To';");
        assertTrue(query.contains("[OK]"));
        assertFalse(query.contains("[ERROR]"));
        String select = sendCommandToServer("SELECT * FROM people;");
        assertTrue(select.contains("24"));
        assertTrue(select.contains("Tom"));
    }


    @Test
    public void testUpdateWithInvalidCondition() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE test (name, age);");
        sendCommandToServer("INSERT INTO test VALUES ('Tom', 28);");
        String result = sendCommandToServer("UPDATE test SET name = 'Rat' WHERE stupid = 'Tom';");
        assertFalse(result.contains("[OK]"));
        assertTrue(result.contains("[ERROR]"));
    }


    @Test
    public void testSuccessfulUpdate() {
            String dbName = generateRandomName();
            sendCommandToServer("CREATE DATABASE " + dbName + ";");
            sendCommandToServer("USE " + dbName + ";");
            sendCommandToServer("CREATE TABLE testTable (name, age);");
            sendCommandToServer("INSERT INTO testTable VALUES ('JohnDoe', 30);");
            String updateResult = sendCommandToServer("UPDATE testTable SET name = 'JohnSmith' WHERE name == 'JohnDoe';");
            assertTrue(updateResult.contains("[OK]"));
            String selectResult = sendCommandToServer("SELECT name FROM testTable WHERE id == 1;");
            assertTrue(selectResult.contains("'JohnSmith'"));
    }

    @Test
    public void testInsertAndUpdate() {
        String randDbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randDbName + ";");
        sendCommandToServer("USE " + randDbName + ";");
        String cmdResB = sendCommandToServer("CREATE TABLE students (name, mark, pass);");
        assertTrue(cmdResB.contains("[OK]"));
        assertFalse(cmdResB.contains("[ERROR]"));
        String insertQuery =  sendCommandToServer("INSERT INTO students VALUES ('saf', 68, TRUE);");
        sendCommandToServer("INSERT INTO students VALUES ('tom', 73, TRUE);");
        assertTrue(insertQuery.contains("[OK]"));
        assertFalse(insertQuery.contains("[ERROR]"));
        String updateQuery = sendCommandToServer("UPDATE students SET mark = 89 WHERE name LIKE 'saf';");
        assertTrue(updateQuery.contains("[OK"));
        assertFalse(updateQuery.contains("[ERROR"));
        String selectResponse = sendCommandToServer("SELECT * from students where name like 'tom';");
        assertTrue(selectResponse.contains("[OK"));
        assertFalse(selectResponse.contains("[ERROR"));
        assertTrue(selectResponse.contains("'tom'"));
        assertTrue(selectResponse.contains("73"));
    }


    // ========================================  DELETE  ======================================== //
    @Test
    public void testDeleteWithNonExistingColumnCondition() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE testTable (name, age);");
        sendCommandToServer("INSERT INTO testTable VALUES ('Rat', 30);");
        String result = sendCommandToServer("DELETE FROM testTable WHERE nonExistingColumn = 'Rat';");
        String result2 = sendCommandToServer("DELETE FROM testTable WHERE name = 'value';");
        assertFalse(result.contains("[OK]"));
        assertTrue(result.contains("[ERROR]"));
        assertFalse(result2.contains("[OK]"));
        assertTrue(result2.contains("[ERROR]"));
    }

    @Test
    public void testDeleteGreaterThan() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE testTable (name, age);");
        sendCommandToServer("INSERT INTO testTable VALUES ('Rat', 30);");
        sendCommandToServer("INSERT INTO testTable VALUES ('tom', 45);");
        sendCommandToServer("INSERT INTO testTable VALUES ('tom', 45);");
        String result = sendCommandToServer("DELETE FROM testTable WHERE age > 30;");
        assertTrue(result.contains("[OK]"));
        assertFalse(result.contains("[ERROR]"));
        String selectCmd = sendCommandToServer("SELECT * FROM testTable;");
        assertTrue(selectCmd.contains("'Rat'"));
        assertFalse(selectCmd.contains("45"));
    }

    @Test
    public void testDeleteGreaterOrEquals() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE test (name, age);");
        sendCommandToServer("INSERT INTO test VALUES ('Rat', 30);");
        sendCommandToServer("INSERT INTO test VALUES ('tom', 45);");
        sendCommandToServer("INSERT INTO test VALUES ('tom', 45);;");
        sendCommandToServer("INSERT INTO test VALUES ('bug', 29);");
        String result = sendCommandToServer("DELETE FROM test WHERE age >= 30;");
        assertTrue(result.contains("[OK]"));
        assertFalse(result.contains("[ERROR]"));
        String selectCmd = sendCommandToServer("SELECT * FROM test;");
        assertFalse(selectCmd.contains("'Rat'"));
        assertFalse(selectCmd.contains("45"));
        assertFalse(selectCmd.contains("'tom'"));
    }


    // =================== STRING LITERAL PARSING TESTS =================== //

    @Test
    public void testStringLiteralsParsing() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE people (name, age);");
        sendCommandToServer("INSERT INTO people VALUES ('tom sanders', 26);");
        sendCommandToServer("INSERT INTO people VALUES ('hayley', 57);");
        sendCommandToServer("INSERT INTO people VALUES ('Safia Scarlett Mirzai', 23);;");
        sendCommandToServer("INSERT INTO people VALUES ('bug', 1781);");
        String selectCmd = sendCommandToServer("SELECT * FROM people;");
        assertTrue(selectCmd.contains("'Safia Scarlett Mirzai'"));

    }


    // =============== Alter ============== //

    @Test
    public void alterAddTableNoInitialColumnHeaders() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE people;");
        String addQuery = sendCommandToServer("ALTER TABLE people add name;");
        assertTrue(addQuery.contains("[OK]"));
        assertFalse(addQuery.contains("[ERROR]"));
        sendCommandToServer("INSERT INTO people VALUES ('tom sanders');");
        sendCommandToServer("INSERT INTO people VALUES ('Safia Scarlett Mirzai');");
        String selectCmd = sendCommandToServer("SELECT * FROM people;");
        assertTrue(selectCmd.contains("'Safia Scarlett Mirzai'"));
        assertTrue(selectCmd.contains("name"));
    }

    @Test
    public void alterTooManyAttributes() {
        String dbName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + dbName + ";");
        sendCommandToServer("USE " + dbName + ";");
        sendCommandToServer("CREATE TABLE people;");
        String addQuery = sendCommandToServer("ALTER TABLE people add name, age;");
        assertFalse(addQuery.contains("[OK]"));
        assertTrue(addQuery.contains("[ERROR]"));
    }


    @AfterAll
    public static void cleanUp() throws IOException {
        String storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        File forCleaning = new File(storageFolderPath);
        for (File file : forCleaning.listFiles()) {
                deleteDirectory(file);
        }
    }

    private static void deleteDirectory(File directory) throws IOException {
        File[] tables = directory.listFiles();
        if (tables != null) {
            for (File table : tables) {
                table.delete();
            }
        }
        directory.delete();
    }


}