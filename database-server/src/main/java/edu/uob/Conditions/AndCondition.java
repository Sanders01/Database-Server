package edu.uob.Conditions;

import edu.uob.DataStructure.Row;
import edu.uob.Exceptions.CmdExecutionException;

public class AndCondition extends Condition {

    private final Condition leftCondition;
    private final Condition rightCondition;

    public AndCondition(Condition leftCondition, Condition rightCondition) {
        this.leftCondition = leftCondition;
        this.rightCondition = rightCondition;
    }

    @Override
    public boolean evaluateCondition(Row row) throws CmdExecutionException {
        return leftCondition.evaluateCondition(row) && rightCondition.evaluateCondition(row);
    }



}