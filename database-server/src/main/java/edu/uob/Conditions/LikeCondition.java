package edu.uob.Conditions;
import edu.uob.DataStructure.Row;
import edu.uob.DataStructure.Table;
import edu.uob.Exceptions.CmdExecutionException;
import java.util.regex.Pattern;

public class LikeCondition extends Condition {

    private final String columnName;
    private final String patternRegex;
    private final Pattern compiledPattern;
    private final Table table;


    public LikeCondition(Table table, String columnName, String pattern) {
        this.table = table;
        this.columnName = columnName;
        // remove quotes if present
        if (pattern.startsWith("'") && pattern.endsWith("'")) {
            pattern = pattern.substring(1, pattern.length() - 1);
        }
        this.patternRegex = Pattern.quote(pattern)
                .replace("%", "\\E.*\\Q") // Replace SQL LIKE wildcard % with regex .*
                .replace("_", "\\E.\\Q"); // Replace SQL LIKE single-character wildcard _ with regex .
        this.compiledPattern = Pattern.compile(this.patternRegex, Pattern.CASE_INSENSITIVE);
    }


    @Override
    public boolean evaluateCondition(Row row) throws CmdExecutionException {
        String cellValue = row.getCellData(columnName, table);
        return compiledPattern.matcher(cellValue).find(); // use find() forsubstring matches
    }
}


