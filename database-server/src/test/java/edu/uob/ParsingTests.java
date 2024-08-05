package edu.uob;
import edu.uob.Commands.CreateDatabase;
import edu.uob.Commands.UseDatabase;
import edu.uob.Exceptions.CmdExecutionException;
import edu.uob.Exceptions.ParseException;
import edu.uob.Parsing.Parser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.IOException;


import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ParsingTests {

    private DBServer server;
    @BeforeEach
    public void setup() {
        server = new DBServer();
    }

    // ========================= USE ========================= //
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


    // ========================= SELECT ========================= //


    @Test
    public void selectNoFrom() {
        DBServer server = null;
        String[] tokens = {"SELECT", "*", "users", ";"};
        Parser parser = new Parser(server, tokens);
        assertThrows(ParseException.class, () -> parser.parseSelect());
    }

    @Test
    public void selectNoFromName() {
        DBServer server = null;
        String[] tokens = {"SELECT", "*", "table", ";"};
        Parser parser = new Parser(server, tokens);
        assertThrows(ParseException.class, () -> parser.parseSelect());
    }



    @Test
    public void selectNoSemiColon() throws CmdExecutionException, IOException {
        DBServer server = null;
        String[] tokens = {"SELECT", "*", "FROM", "table"};
        Parser parser = new Parser(server, tokens);
        assertThrows(ParseException.class, () -> parser.parseTokens(tokens));
    }


    // ========================= parseAttributeList Tests ========================= //








    // ========================= Helper Method Tests ========================= //

        @Test
        public void testParseAttributeName() {
            DBServer server = null;
            String[] tokens = {"SELECT", "*", "FROM", "table"};
            Parser parser = new Parser(server, tokens);
            assertTrue(parser.parseAttributeName("name"));
            assertTrue(parser.parseAttributeName("orderdate"));
            assertTrue(parser.parseAttributeName("cOluMn2"));
            assertFalse(parser.parseAttributeName("na me"));
            assertFalse(parser.parseAttributeName("orderdate?"));
            assertFalse(parser.parseAttributeName(" cOluMn2"));
        }

    @Test
    public void testParseValue() {
        String[] tokens = null;
        Parser parser = new Parser(server, tokens);
        assertTrue(parser.isBoolLiteral("TRUE"));
        assertTrue(parser.isBoolLiteral("FALSE"));
        assertTrue(parser.isBoolLiteral("true"));
        assertTrue(parser.isBoolLiteral("fAlse"));
        assertFalse(parser.isBoolLiteral("tr ue"));
        assertTrue(parser.isStringLiteral("'This is a string'"));
        assertTrue(parser.isStringLiteral("'123'"));
        assertFalse(parser.isStringLiteral("NotAString"));
        assertFalse(parser.isStringLiteral("'Unmatched quotes"));

    }





}