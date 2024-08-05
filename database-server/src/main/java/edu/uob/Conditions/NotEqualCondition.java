package edu.uob.Conditions;

import edu.uob.DataStructure.Row;
import edu.uob.DataStructure.Table;
import edu.uob.Exceptions.CmdExecutionException;

public class NotEqualCondition extends Condition {

    private String columnName;
    private String value;
    private Table table;

    public NotEqualCondition(Table table, String columnName, String value) {
        this.table = table;
        this.columnName = columnName;
        this.value = value;
    }

    @Override
    public boolean evaluateCondition(Row row) throws CmdExecutionException {
        String cellValue = row.getCellData(columnName, table);
        return !value.equals(cellValue);
    }


}

