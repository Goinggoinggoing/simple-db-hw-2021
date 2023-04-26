package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private static final Field NO_GROUP = new IntField(-1);

    private TupleDesc tupleDesc;

    private ConcurrentHashMap<Field, Tuple> aggregate;

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
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field operatorField = tup.getField(afield);

        if(operatorField == null){
            return ;
        }

        if (gbfield == NO_GROUPING){
            Tuple tuple = aggregate.get(NO_GROUP);
            this.aggregate.put(NO_GROUP, tuple);
            if (tuple == null){
                tuple = new Tuple(tupleDesc);
                if(what.equals(Op.COUNT)){
                    tuple.setField(0, new IntField(1));
                }
                aggregate.put(NO_GROUP, tuple);
                return ;
            }
            Field f = tuple.getField(0);

            switch (what){
                case COUNT:
                    tuple.setField(0, new IntField(((IntField) f).getValue() + 1));
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
                }
                aggregate.put(groupField, tmpTuple);
                return ;
            }
            Field f = tuple.getField(1);
            switch (what){
                case COUNT:
                    int value = ((IntField) f).getValue();
                    tuple.setField(1, new IntField(value + 1));
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
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // throw new UnsupportedOperationException("please implement me for lab2");
        return new StringIterator(this);
    }


    public class StringIterator implements OpIterator {
        private StringAggregator aggregator;
        private Iterator<Tuple> iterator;

        public StringIterator(StringAggregator aggregator) {
            this.aggregator = aggregator;
            this.iterator = null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.iterator = aggregator.aggregate.values().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            iterator = aggregator.aggregate.values().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggregator.tupleDesc;
        }

        @Override
        public void close() {
            iterator = null;
        }
    }

}
