package edu.uob.Conditions;

import edu.uob.DataStructure.Row;
import edu.uob.Exceptions.CmdExecutionException;

public abstract class Condition {


    public Condition() {
    }

    public abstract boolean evaluateCondition(Row row) throws CmdExecutionException;



}
