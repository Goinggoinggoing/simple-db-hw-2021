package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int w;
    private int num_buckets;
    private int min;
    private int max;
    private int[] buckets;
    private int range;
    private int total;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.num_buckets = buckets;
        this.min = min;
        this.max = max;
        this.buckets = new int[buckets];
        this.range = max - min + 1;
        this.w = (int) Math.ceil((double) range / buckets);
        this.total = 0;
    }

    private int getIndex(int v){
        return (v-this.min)/w;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        buckets[getIndex(v)] ++ ;
        total++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        if(op.equals(Predicate.Op.EQUALS)){
            if(v < min || v>max){
                return 0;
            }
            return (double) buckets[getIndex(v)] / w / total;
        }
        if(op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ)){
            if(v < min){
                return 1;
            }else if (v > max){
                return 0;
            }
            // 包含自己时多移位一次
            int t = op.equals(Predicate.Op.GREATER_THAN_OR_EQ) ? 1 : 0;
            int idx = getIndex(v);
            int bRigth = w * (idx + 1) + min - 1;
            double b_part  = (double) (bRigth - v + 1) / w * buckets[idx];
            for(int i = idx+1; i < num_buckets; i++){
                b_part += buckets[i];
            }
            return b_part / total;
        }
        if(op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ)){
            if(v < min){
                return 0;
            }else if (v > max){
                return 1;
            }
            int t = op.equals(Predicate.Op.LESS_THAN_OR_EQ) ? 1 : 0;

            int idx = getIndex(v);
            int bLeft = w * (idx) + min;
            double b_part  = (double) (v - bLeft + t) / w * buckets[idx];
            for(int i = idx-1; i >= 0; i--){
                b_part += buckets[i];
            }
            return b_part / total;
        }
        if (op.equals(Predicate.Op.NOT_EQUALS)){
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder s = new StringBuilder("");

        for (int i = 0; i < buckets.length; i++) {
            s.append(i + ": " + buckets[i] + "\n");
        }

        return s.toString();
    }
}
