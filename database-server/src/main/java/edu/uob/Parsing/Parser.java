package edu.uob.Parsing;

import edu.uob.Commands.*;
import edu.uob.Conditions.*;
import edu.uob.DBCommand;
import edu.uob.DBServer;
import edu.uob.DataLoader;
import edu.uob.DataStructure.Table;
import edu.uob.Exceptions.CmdExecutionException;
import edu.uob.Exceptions.ParseException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


public class Parser {

    private final DBServer server;

    private final String[] tokens;

    private int tokenIndex = 0;

    public Parser(DBServer server, String[] tokens) {
        this.tokens = tokens;
        this.server = server;

    }

    public enum Comparator {
        EQUALS("=="),
        GREATER_THAN(">"),
        LESS_THAN("<"),
        GREATER_OR_EQUAL(">="),
        LESS_OR_EQUAL("<="),
        NOT_EQUAL("!="),
        LIKE("LIKE");

        private final String symbol;

        Comparator(String symbol) {
            this.symbol = symbol;
        }

        public String getSymbol() {
            return symbol;
        }

        public static Comparator determineComparator(String symbol) throws ParseException {
            for (Comparator comparator : values()) {
                if (comparator.getSymbol().equalsIgnoreCase(symbol)) {
                    return comparator;
                }
            }
            throw new ParseException("Invalid comparator symbol: " + symbol);
        }
    }

    private boolean isValidComparator(String token) {
        final String[] validComparators = {"<", ">", "<=", ">=", "!=", "==", "LIKE"};
        for (String comparator : validComparators) {
            if (token.equalsIgnoreCase(comparator)) {
                return true;
            }
        }
        return false;
    }


    public DBCommand parseTokens(String[] tokens) throws ParseException, CmdExecutionException, IOException {

        if (tokens.length == 0) {
            throw new ParseException("Empty query.");
        }
        String finalToken = tokens[tokens.length - 1];
        if (!finalToken.equals(";")) {
            throw new ParseException("Expected a semicolon following a query.");
        }

        String commandType = tokens[tokenIndex].toUpperCase();
        return switch (commandType) {
            case "USE" -> parseUse();
            case "CREATE" -> parseCreate();
            case "INSERT" -> parseInsert();
            case "DROP" -> parseDrop();
            case "ALTER" -> parseAlter();
            case "SELECT" -> parseSelect();
            case "JOIN" -> parseJoin();
            case "DELETE" -> parseDelete();
            case "UPDATE" -> parseUpdate();
            default -> throw new ParseException("Unknown or invalid query.");
        };
    }


    // ------------------------------- Individual Parsing Methods -------------------------------- //


    private DBCommand parseJoin() throws ParseException, CmdExecutionException {
        String databaseName = server.getCurrentDatabase();
        tokenIndex++; // move past 'JOIN'
        String firstTableName = tokens[tokenIndex];
        tokenIndex++; // move past the first table name
        if (!tokens[tokenIndex].equalsIgnoreCase("AND")) {
            throw new ParseException("Expected 'AND' instead found: " + tokens[tokenIndex]);
        }
        tokenIndex++; // move past 'AND'
        String secondTableName = tokens[tokenIndex];
        if (!server.tableExists(databaseName, firstTableName) || !server.tableExists(databaseName, secondTableName)) {
            throw new CmdExecutionException("One or both of the tables do not exist in the database.");
        }
        tokenIndex++; // move past second table name
        if (!tokens[tokenIndex].equalsIgnoreCase("ON")) {
            throw new ParseException("Expected 'ON' instead found: " + tokens[tokenIndex]);
        }
        tokenIndex++; // move past 'ON'
        String firstAttribute = tokens[tokenIndex];
        tokenIndex++; // move past first attribute
        if (!tokens[tokenIndex].equalsIgnoreCase("AND")) {
            throw new ParseException("Expected 'AND' instead found: " + tokens[tokenIndex]);
        }
        tokenIndex++; // move past 'AND'
        String secondAttribute = tokens[tokenIndex];
        return new JoinCommand(firstTableName, secondTableName, firstAttribute, secondAttribute);
    }



    private boolean isBoolOperatorOrComparator(String token) {
        return isBoolOperator(token) || isValidComparator(token);
    }


    public Condition parseCondition(Table table) throws ParseException {
        boolean hasBrackets = tokens[tokenIndex].equals("(");
        if (hasBrackets) {
            tokenIndex++; // move past '('
        }
        Condition condition = null;
        condition = parseSimpleCondition(table);
        // check for boolean operator indicating nested conditions
        while (tokenIndex < tokens.length && isBoolOperator(tokens[tokenIndex])) {
            String boolOperator = tokens[tokenIndex++];
            if (!isBoolOperatorOrComparator(boolOperator)) {
                throw new ParseException("Invalid operator or comparator.");
            }
            Condition rightCondition = parseSimpleCondition(table);
            if (boolOperator.equalsIgnoreCase("AND")) {
                condition = new AndCondition(condition, rightCondition);
            } else if (boolOperator.equalsIgnoreCase("OR")) {
                condition = new OrCondition(condition, rightCondition);
            }
        }
        if (hasBrackets) {
            if (!tokens[tokenIndex].equals(")")) {
                throw new ParseException("Expected ')' but found: " + tokens[tokenIndex]);
            }
            tokenIndex++; // move past ')'
        }
        return condition;
    }


    private boolean isBoolOperator(String token) {
        return token.equalsIgnoreCase("AND") || token.equalsIgnoreCase("OR");
    }

    public Condition parseSimpleCondition(Table table) throws ParseException {
        String attributeName = tokens[tokenIndex++];
        String comparatorSymbol = tokens[tokenIndex];
        // Handle possible composite symbols
        if (tokenIndex + 1 < tokens.length) { // Ensure there's another token to check
            String nextToken = tokens[tokenIndex + 1];
            if (comparatorSymbol.equals("<") && nextToken.equals("=")) {
                comparatorSymbol = "<=";
                tokenIndex += 2; // move past the combined tokens
            } else if (comparatorSymbol.equals(">") && nextToken.equals("=")) {
                comparatorSymbol = ">=";
                tokenIndex += 2;
            } else if (comparatorSymbol.equals("!") && nextToken.equals("=")) {
                comparatorSymbol = "!=";
                tokenIndex += 2;
            } else {
                tokenIndex++; //  move past the comparator symbol
            }
        }
        String value = tokens[tokenIndex++];
        Comparator comparator = Comparator.determineComparator(comparatorSymbol);
        return switch (comparator) {
            case EQUALS -> new EqualsCondition(table, attributeName, value);
            case GREATER_THAN -> new GreaterCondition(table, attributeName, value);
            case GREATER_OR_EQUAL -> new GreaterOrEqualCondition(table, attributeName, value);
            case LESS_THAN -> new LessCondition(table, attributeName, value);
            case LESS_OR_EQUAL -> new LessOrEqualCondition(table, attributeName, value);
            case NOT_EQUAL -> new NotEqualCondition(table, attributeName, value);
            case LIKE -> new LikeCondition(table, attributeName, value);
        };
    }


    public DBCommand parseSelect() throws ParseException, IOException {
        tokenIndex++; // move past 'SELECT'
        ArrayList<String> columnNames = new ArrayList<>();
        boolean selectAll = false;
        if (tokens[tokenIndex].equals("*")) {
            selectAll = true;
            tokenIndex++; // move past "*"
        } else {
            columnNames = parseAttributeList();
        }
        if (!tokens[tokenIndex].equalsIgnoreCase("FROM")) {
            throw new ParseException("Expected 'FROM' instead found: " + tokens[tokenIndex]);
        }
        tokenIndex++; // move past 'FROM'
        String tableName = tokens[tokenIndex];
        String databaseName = server.getCurrentDatabase();
        DataLoader loader = new DataLoader(server);
        Table table = loader.readTableData(databaseName, tableName);
        tokenIndex++; // move past the table name
        if (tokens[tokenIndex].equals(";")) {
            if (selectAll) {
                return new SelectCommand(table, selectAll);
            } else {
                return new SelectCommand(table, columnNames);
            }
        }
        if (!tokens[tokenIndex].equalsIgnoreCase("WHERE")) {
            throw new ParseException("Expected a 'WHERE' in this query, instead found: " + tokens[tokenIndex]);
        }
        tokenIndex++; // Move past 'WHERE'
        Condition condition = parseCondition(table);
        if (selectAll) {
            return new SelectCommand(table, true, condition);
        } else {
            return new SelectCommand(table, columnNames, condition);
        }
    }


    public ArrayList<String> parseAttributeList() throws ParseException {
        ArrayList<String> columnNames = new ArrayList<>();
        while (true) {
            if (tokens[tokenIndex].equals(")") || tokens[tokenIndex].equalsIgnoreCase("FROM")) {
                return columnNames;
            }
            if (parseAttributeName(tokens[tokenIndex])) {
                columnNames.add(tokens[tokenIndex]);
                tokenIndex++;

                if (",".equals(tokens[tokenIndex])) {
                    tokenIndex++; // Move past comma
                }
            } else {
                throw new ParseException("Invalid attribute name: " + tokens[tokenIndex]);
            }
        }
    }


    public boolean parseAttributeName(String token) {
        return !token.contains(" ") && isPlainText(token);
    }


    public boolean isPlainText(String token) {
        return token.matches("^[a-zA-Z0-9 ]+$");
    }


    public boolean isValidPlainText(String token) {
        return !token.contains(" ") && isPlainText(token);
    }


    public DBCommand parseUpdate() throws ParseException, CmdExecutionException, IOException {
        tokenIndex++; // move past 'UPDATE' keyword
        String databaseName = server.getCurrentDatabase();
        String tableName = tokens[tokenIndex];
        if (!server.tableExists(databaseName, tableName)) {
            throw new CmdExecutionException("That table name doesnt exist in the current database.");
        }
        tokenIndex++; // move past table name
        if (!tokens[tokenIndex].equalsIgnoreCase("SET")) {
            throw new ParseException("Expected 'SET' in an 'UPDATE' query.");
        }
        tokenIndex++; // move past 'SET'
        HashMap<String, String> updates = parseNameValueList();
        tokenIndex++; // move past the list of values
        if (!tokens[tokenIndex].equalsIgnoreCase("WHERE")) {
            throw new ParseException("Expected 'WHERE' in 'UPDATE' query, instead found: " + tokens[tokenIndex]);
        }
        tokenIndex++; // move past 'WHERE'
        DataLoader loader = new DataLoader(server);
        Table table = loader.readTableData(databaseName, tableName);
        Condition condition = parseCondition(table);
        return new UpdateCommand(table, updates, condition);
    }


    public HashMap<String, String> parseNameValueList() throws ParseException {
        HashMap<String, String> updates = new HashMap<>();

        NameValuePair firstPair = parseNameValuePair();
        updates.put(firstPair.getAttributeName(), firstPair.getValue());
        while (tokens[tokenIndex].equals(",")) {
            tokenIndex++; // Move past the comma
            NameValuePair nextPair = parseNameValuePair();
            updates.put(nextPair.getAttributeName(), nextPair.getValue());
        }

        return updates;
    }


    public NameValuePair parseNameValuePair() throws ParseException {
        if (!parseAttributeName(tokens[tokenIndex])) {
            throw new ParseException("Invalid attribute name: " + tokens[tokenIndex]);
        }
        String attributeName = tokens[tokenIndex];
        tokenIndex++;

        if (!tokens[tokenIndex].equals("=")) {
            throw new ParseException("Expected '=' in a name-value pair, found: " + tokens[tokenIndex]);
        }
        tokenIndex++; // move past equals

        String value = parseValue(tokens[tokenIndex]);

        return new NameValuePair(attributeName, value);
    }


    public DBCommand parseDelete() throws ParseException, CmdExecutionException, IOException {
        tokenIndex++; // move past 'DELETE
        if (!tokens[tokenIndex].equalsIgnoreCase("FROM")) {
            throw new ParseException("Expected 'FROM' following a 'DELETE' query.");
        }
        tokenIndex++; // move past from
        String databaseName = server.getCurrentDatabase();
        String tableName = tokens[tokenIndex];
        if (!server.tableExists(databaseName, tableName)) {
            throw new CmdExecutionException("That table name doesnt exist in the current database.");
        }
        tokenIndex++; // move past table name
        if (!tokens[tokenIndex].equalsIgnoreCase("WHERE")) {
            throw new ParseException("Expected 'WHERE' after the table name in a delete query.");
        }
        tokenIndex++; // move past 'WHERE'
        DataLoader loader = new DataLoader(server);
        Table table = loader.readTableData(databaseName, tableName);
        Condition condition = parseCondition(table);

        return new DeleteCommand(table, condition);
    }


    public DBCommand parseAlter() throws ParseException, CmdExecutionException {
        tokenIndex++; // move past "ALTER"
        if (!tokens[tokenIndex].equalsIgnoreCase("TABLE")) {
            throw new ParseException("Expected 'TABLE' following 'ALTER'.");
        }
        tokenIndex++; // move past "TABLE"
        String databaseName = server.getCurrentDatabase().toLowerCase();
        String tableName = tokens[tokenIndex].toLowerCase();
        if (!server.tableExists(databaseName, tableName)) {
            throw new ParseException("The table doesnt exist in the current database.");
        }
        tokenIndex++; // move past table name
        if (!isAlterationType(tokens[tokenIndex])) {
            throw new ParseException("Invalid alteration type in 'ALTER' query.");
        }
        tokenIndex++;
        String columName = tokens[tokenIndex];
        if (!tokens[tokenIndex + 1].equals(";")) {
            throw new ParseException("Invalid number of attribute names in ALTER query.");
        }
        if (tokens[tokenIndex - 1].equalsIgnoreCase("ADD")) {
            return new AlterAdd(tableName, columName);
        } else if (tokens[tokenIndex - 1].equalsIgnoreCase("DROP")) {
            return new AlterDrop(tableName, columName);
        } else {
            throw new ParseException("Issue parsing 'ALTER' query.");
        }
    }


    private boolean isAlterationType(String token) {
        return token.equalsIgnoreCase("ADD") || token.equalsIgnoreCase("DROP");
    }


    public DBCommand parseDrop() throws ParseException, CmdExecutionException {
        tokenIndex++; // move past drop
        if (tokens[tokenIndex].equalsIgnoreCase("DATABASE")) {
            return parseDropDatabase();
        } else if (tokens[tokenIndex].equalsIgnoreCase("TABLE")) {
            return parseDropTable();
        } else {
            throw new ParseException("Expected 'DATABASE' or 'TABLE' in drop command.");
        }
    }


    public DBCommand parseDropTable() throws CmdExecutionException {
        tokenIndex++; // move past "TABLE"
        String tableName = tokens[tokenIndex].toLowerCase();
        String databaseName = server.getCurrentDatabase().toLowerCase();
        if (!server.tableExists(databaseName, tableName)) {
            throw new CmdExecutionException("Table not found in currently in use database.");
        } else {
            return new DropTable(tableName);
        }
    }


    public DBCommand parseDropDatabase() throws ParseException {
        tokenIndex++; // move past "DATABASE"
        if (tokenIndex >= tokens.length) {
            throw new ParseException("Expected database name after 'DATABASE'");
        }
        String databaseName = tokens[tokenIndex].toLowerCase();

        if (!server.databaseExists(databaseName)) {
            throw new ParseException("That database does not exist");
        }

        return new DropDatabase(databaseName);
    }


    public DBCommand parseInsert() throws ParseException, CmdExecutionException {
        tokenIndex++; // move past "INSERT"
        if (!tokens[tokenIndex].equalsIgnoreCase("INTO")) {
            throw new ParseException("Expected 'INTO' following an 'INSERT' query.");
        }
        tokenIndex++;
        String tableName = tokens[tokenIndex].toLowerCase();
        String databaseName = server.getCurrentDatabase().toLowerCase();
        if (!server.tableExists(databaseName, tableName)) {
            throw new ParseException("That table doesnt exist in the current Database.");
        }
        tokenIndex++; // move past the table name

        if (!tokens[tokenIndex].equalsIgnoreCase("VALUES")) {
            throw new ParseException("Expected 'VALUES' following the table name.");
        }
        tokenIndex++;
        ArrayList<String> values = parseValueList();

        return new InsertCommand(tableName, values);

    }


    private ArrayList<String> parseValueList() throws ParseException {
        ArrayList<String> values = new ArrayList<>();
        if (!tokens[tokenIndex].equals("(")) {
            throw new ParseException("Expected: '('.");
        }
        tokenIndex++;
        while (!tokens[tokenIndex].equals(")")) {
            String value = parseValue(tokens[tokenIndex]);
            values.add(value);
            tokenIndex++; // Move past the value
            if (",".equals(tokens[tokenIndex])) {
                tokenIndex++; // Move past comma
            } else if (!tokens[tokenIndex].equals(")")) {
                throw new ParseException("Expected ',' or ')' after a value, instead found: " + tokens[tokenIndex]);
            }
        }
        tokenIndex++;  // Move past the closing parenthesis

        if (!tokens[tokenIndex].equals(";")) {
            throw new ParseException("Invalid token: " + tokens[tokenIndex] + " in value list.");
        }
        return values;
    }


    public String parseValue(String value) throws ParseException {

        if (value.equalsIgnoreCase("NULL")) {
            return " ";
        }
        if (isStringLiteral(value)) {
            return value;
        }

        if (isBoolLiteral(value)) {
            return value;
        }

        if (isIntegerLiteral(value)) {
            return value;
        }

        if (isFloatLiteral(value)) {
            return value;

        } else {
            throw new ParseException("Unknown value type entered.");
        }
    }


    public boolean isStringLiteral(String token) {
        return token.startsWith("'") && token.endsWith("'");
    }

    // [IntegerLiteral]  ::=  [DigitSequence] | "-" [DigitSequence] | "+" [DigitSequence]
    public boolean isIntegerLiteral(String token) {
        return token.matches("^[+-]?\\d+$");
    }

    public boolean isFloatLiteral(String token) {
        return token.matches("^[+-]?\\d+\\.\\d+$");
    }

    public boolean isBoolLiteral(String token) {
        return token.equalsIgnoreCase("TRUE") || token.equalsIgnoreCase("FALSE");
    }


    public DBCommand parseUse() throws ParseException {
        tokenIndex++; // move past "USE" keyword
        if (tokens.length != 3) {
            throw new ParseException("Insufficient number of tokens for USE command.");
        }
        String databaseName = tokens[tokenIndex].toLowerCase();
        return new UseDatabase(databaseName);
    }


    public DBCommand parseCreate() throws ParseException, CmdExecutionException {
        tokenIndex++; //move past "CREATE"
        String currentToken = tokens[tokenIndex];
        if (currentToken.equalsIgnoreCase("DATABASE")) {
            tokenIndex++;
            String databaseName = tokens[tokenIndex].toLowerCase();
            return new CreateDatabase(databaseName);
        } else if (currentToken.equalsIgnoreCase("TABLE")) {
            return parseCreateTable();
        } else {
            throw new ParseException("Expected 'DATABASE' or 'TABLE' following a CREATE statement.");
        }
    }


    private DBCommand parseCreateTable() throws ParseException, CmdExecutionException {
        ArrayList<String> emptyColumnNames = new ArrayList<>();
        tokenIndex++; // move past "TABLE"
        if (tokenIndex >= tokens.length || !isValidPlainText(tokens[tokenIndex])) {
            throw new ParseException("Invalid table name.");
        }
        String tableName = tokens[tokenIndex].toLowerCase();
        tokenIndex++; // move past table name
        if (tokens[tokenIndex].equals(";")) {
            return new CreateTable(tableName, emptyColumnNames);
        }
        if (!tokens[tokenIndex].equals("(")) {
            throw new ParseException("Expected: '(' before the attribute list, instead found: " + tokens[tokenIndex]);
        }
        tokenIndex++; // move past the ")"
        ArrayList<String> columNames = parseAttributeList();

        return new CreateTable(tableName, columNames);
    }

}
