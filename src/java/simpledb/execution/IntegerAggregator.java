package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private static final Field NO_GROUP = new IntField(-1);

    private TupleDesc tupleDesc;

    private ConcurrentHashMap<Field, Tuple> aggregate;

    private int counts;
    private int summary;

    private ConcurrentHashMap<Field, Integer> countsMap;
    private ConcurrentHashMap<Field, Integer> sumMap;

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
        this.aggregate = new ConcurrentHashMap<Field, Tuple>();

        if (gbfield == NO_GROUPING) {
            this.tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"aggregateValue"});

        } else {
            this.tupleDesc = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE}, new String[] {"groupValue", "aggregateValue"});
        }
        if (gbfield == NO_GROUPING && what.equals(Op.AVG)) {
            this.counts = 0;
            this.summary = 0;
        } else if (gbfield != NO_GROUPING && what.equals(Op.AVG)) {
            this.countsMap = new ConcurrentHashMap<>();
            this.sumMap = new ConcurrentHashMap<>();
        }
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
        IntField operatorField = (IntField)tup.getField(afield);

        if(operatorField == null){
            return ;
        }

        if (gbfield == NO_GROUPING){
            Tuple tuple = aggregate.get(NO_GROUP);
            if (tuple == null){
                tuple = new Tuple(tupleDesc);
                if(what.equals(Op.COUNT)){
                    tuple.setField(0, new IntField(1));
                }else if (what.equals(Op.AVG)){
                    counts = 1;
                    summary = operatorField.getValue();
                    tuple.setField(0, operatorField);
                }else{
                    tuple.setField(0, operatorField);
                }
                aggregate.put(NO_GROUP, tuple);
                return ;
            }
            Field f = tuple.getField(0);
            switch (what){
                case MIN:
                    if (operatorField.compare(Predicate.Op.LESS_THAN, f)){
                        tuple.setField(0, operatorField);
                        aggregate.put(NO_GROUP, tuple);
                    }
                    return;
                case MAX:
                    if (operatorField.compare(Predicate.Op.GREATER_THAN, f)){
                        tuple.setField(0, operatorField);
                        aggregate.put(NO_GROUP, tuple);
                    }
                    return;
                case COUNT:
                    tuple.setField(0, new IntField(((IntField) f).getValue() + 1));
                    aggregate.put(NO_GROUP, tuple);
                    return;
                case SUM:
                    tuple.setField(0, new IntField(((IntField) f).getValue() + operatorField.getValue()));
                    aggregate.put(NO_GROUP, tuple);
                    return;
                case AVG:
                    counts ++;
                    summary += operatorField.getValue();
                    tuple.setField(0, new IntField(summary / counts));
                    aggregate.put(NO_GROUP, tuple);
                    return;
                default:
                    return ;
            }
        }else{
            Field groupField = tup.getField(gbfield);
            Tuple tuple = aggregate.get(groupField);
            if (tuple == null){
                Tuple tmpTuple = new Tuple(tupleDesc);
                tmpTuple.setField(0, groupField);
                if (what.equals(Op.COUNT)){
                    tmpTuple.setField(1, new IntField(1));
                }else if (what.equals(Op.AVG)){
                    countsMap.put(groupField, 1);
                    sumMap.put(groupField, operatorField.getValue());
                    tmpTuple.setField(1, operatorField);
                }else{
                    tmpTuple.setField(1, operatorField);
                }
                aggregate.put(groupField, tmpTuple);
                return ;
            }
            Field f = tuple.getField(1);
            switch (what){
                case MIN:
                    if (operatorField.compare(Predicate.Op.LESS_THAN, f)){
                        tuple.setField(1, operatorField);
                        aggregate.put(groupField, tuple);
                    }
                    return;
                case MAX:
                    if (operatorField.compare(Predicate.Op.GREATER_THAN, f)){
                        tuple.setField(1, operatorField);
                        aggregate.put(groupField, tuple);
                    }
                    return;
                case COUNT:
                    int value = ((IntField) f).getValue();
                    tuple.setField(1, new IntField(value + 1));
                    aggregate.put(groupField, tuple);
                    return;
                case SUM:
                    int sum = ((IntField) f).getValue();
                    tuple.setField(1, new IntField(sum + operatorField.getValue()));
                    aggregate.put(groupField, tuple);
                    return;
                case AVG:
                    sumMap.put(groupField, sumMap.get(groupField) + operatorField.getValue());
                    countsMap.put(groupField, countsMap.get(groupField) + 1);
                    tuple.setField(1, new IntField(sumMap.get(groupField) / countsMap.get(groupField)));
                    aggregate.put(groupField, tuple);
                    return;
                default:
                    return ;
            }
        }

    }

    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
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
//        throw new UnsupportedOperationException("please implement me for lab2");
        return new IntIterator(this);
    }

    public class IntIterator implements OpIterator{


        private final IntegerAggregator aggregator;
        Iterator<Tuple> iter;

        public IntIterator(IntegerAggregator integerAggregator) {
            aggregator = integerAggregator;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iter = aggregator.aggregate.values().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            iter = aggregator.aggregate.values().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return this.aggregator.tupleDesc;
        }

        @Override
        public void close() {
            iter = null;
        }
    }


}
