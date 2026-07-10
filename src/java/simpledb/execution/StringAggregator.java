package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import simpledb.storage.TupleIterator;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    boolean hasGroupings = false;
    private Map<Field, Integer> aggregatedResults;
    private static final Field NO_GROUPING_FIELD = new IntField(NO_GROUPING);
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        hasGroupings = (gbfield != NO_GROUPING);
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT");
        }
        this.aggregatedResults = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
         Field fieldToGroup = NO_GROUPING_FIELD;
        if (hasGroupings) {
            fieldToGroup = tup.getField(gbfield);
        }
        String aggregateVal = tup.getField(afield).toString();
        int count = aggregatedResults.getOrDefault(fieldToGroup, 0);
        aggregatedResults.put(fieldToGroup, count + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc td;
        if (hasGroupings) {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }

        List<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : aggregatedResults.entrySet()) {
            Tuple t = new Tuple(td);
            if (hasGroupings) {
                t.setField(0, entry.getKey());
                t.setField(1, new IntField(entry.getValue()));
            } else {
                t.setField(0, new IntField(entry.getValue()));
            }
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }
}
