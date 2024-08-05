package edu.uob.DataStructure;

import edu.uob.Exceptions.CmdExecutionException;
import java.util.ArrayList;

public class Row {

    private final ArrayList<String> cells = new ArrayList<>();

    public Row(int rowId, ArrayList<String> data) {
        this.cells.add(String.valueOf(rowId));
        this.cells.addAll(data);
    }

    public String getCellData(int index) {
        return cells.get(index);
    }

    public ArrayList<String> getCells() {
        return cells;
    }


    public String getCellData(String columnName, Table table) throws CmdExecutionException {
        int index = table.findColumnIndex(columnName);
        if (index != - 1 && index < cells.size()) {
            return cells.get(index);
        }
        throw new CmdExecutionException("That attribute name isn't part of this table.");
    }


    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < cells.size(); i++) {
            Object currentCell = cells.get(i);
            if (currentCell == null) {
                stringBuilder.append(" ");
            } else {
                stringBuilder.append(currentCell);
            }
            if (i < cells.size() - 1) {
                stringBuilder.append("\t");
            }
        }
        return stringBuilder.toString();
    }

}
