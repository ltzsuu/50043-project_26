package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.TupleDesc;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private boolean hasGroupings = false;
    private Map<Field, Integer> aggregatedResults;
    private Map<Field, Integer> countbyField;
    private static final Field NO_GROUPING_FIELD = new IntField(NO_GROUPING);
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.hasGroupings = (gbfield != NO_GROUPING);
        this.aggregatedResults = new HashMap<>();
        this.countbyField = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field fieldToGroup = NO_GROUPING_FIELD;
        if (hasGroupings) {
        fieldToGroup = tup.getField(this.gbfield);
        }
        int newValue = ((IntField) tup.getField(afield)).getValue();
        switch (this.what) {
        case AVG:
            {
            int prevSum = this.aggregatedResults.getOrDefault(fieldToGroup, 0);
            int prevCount = this.countbyField.getOrDefault(fieldToGroup, 0);
            int newSum =prevSum + newValue;
            aggregatedResults.put(fieldToGroup, newSum);
            countbyField.put(fieldToGroup, prevCount + 1);
            break;
            }
        case MAX:
            {
            int prevValue = this.aggregatedResults.getOrDefault(fieldToGroup, Integer.MIN_VALUE);
            aggregatedResults.put(fieldToGroup, Math.max(prevValue, newValue));
            break;
            }
        case MIN:
            {
            int prevValue = this.aggregatedResults.getOrDefault(fieldToGroup, Integer.MAX_VALUE);
            aggregatedResults.put(fieldToGroup, Math.min(prevValue, newValue));
            break;
            }
        case SUM:
            {
            int prevValue = this.aggregatedResults.getOrDefault(fieldToGroup, 0);
            aggregatedResults.put(fieldToGroup, prevValue + newValue);
            break;
            }
        case COUNT:
            {
            int prevValue = this.aggregatedResults.getOrDefault(fieldToGroup, 0);
            aggregatedResults.put(fieldToGroup, prevValue + 1);
            break;
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc td;
        if (hasGroupings) {
            td = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
        }
        else {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : aggregatedResults.entrySet()) {
            Field key = entry.getKey();
            int aggregateVal;
            if (this.what == Op.AVG) {
                aggregateVal = entry.getValue() / countbyField.get(key);
            } 
            else {
                aggregateVal = entry.getValue();
            }

            Tuple t = new Tuple(td);
            if (!hasGroupings) {
                t.setField(0, new IntField(aggregateVal));
            } 
            else {
                t.setField(0, key);
                t.setField(1, new IntField(aggregateVal));
            }
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }

}
