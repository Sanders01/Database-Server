package edu.uob.Conditions;

import edu.uob.DataStructure.Row;
import edu.uob.DataStructure.Table;
import edu.uob.Exceptions.CmdExecutionException;

public class LessOrEqualCondition extends Condition {

    private final String columnName;
    private final double value;
    private final Table table;

    public LessOrEqualCondition(Table table, String columnName, String value) {
        this.table = table;
        this.columnName = columnName;
        this.value = Double.parseDouble(value);
    }

    @Override
    public boolean evaluateCondition(Row row) throws CmdExecutionException {
        double cellValue = Double.parseDouble(row.getCellData(columnName, table));
        return cellValue <= value;
    }

}
