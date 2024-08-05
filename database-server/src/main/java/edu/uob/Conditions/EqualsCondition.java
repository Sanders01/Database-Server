package edu.uob.Conditions;

import edu.uob.DataStructure.Row;
import edu.uob.DataStructure.Table;
import edu.uob.Exceptions.CmdExecutionException;

public class EqualsCondition extends Condition {

    private final Table table;
   private final String columnName;
   private final String value;

    public EqualsCondition(Table table, String columnName, String value) {
        this.table = table;
        this.columnName = columnName;
        this.value = value;
    }


    @Override
    public boolean evaluateCondition(Row row) throws CmdExecutionException {
        String cellValue = row.getCellData(columnName, table);
        try {
            //  see if both can be treated as integers
            if (cellValue.contains(".") || value.contains(".")) {
                // At least one is a floating point, compare as doubles
                double cellValueDouble = Double.parseDouble(cellValue);
                double conditionValueDouble = Double.parseDouble(value);
                final double EPSILON = 0.000001; // float tolerance
                return Math.abs(cellValueDouble - conditionValueDouble) < EPSILON;
            } else {
                // Both can be treated as integers
                int cellValueInt = Integer.parseInt(cellValue);
                int conditionValueInt = Integer.parseInt(value);
                return cellValueInt == conditionValueInt;
            }
        } catch (NumberFormatException e) {
            return value.equals(cellValue);
        }
    }






}
