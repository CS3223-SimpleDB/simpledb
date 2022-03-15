package simpledb.materialize;

import simpledb.query.*;

/**
 * The <i>average</i> aggregation function.
 */
public class AverageFn implements AggregationFn {
   private String fldname;
   private Constant sum;
   private int count;
   private int average;

   /**
    * Create a average aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public AverageFn(String fldname) {
      this.fldname = fldname;
   }
   
   /**
    * Set the sum as the field value in the first record.
    * Start a new count set to 1.
    * Calculate the average from dividing the sum by the count.
    * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
    */
   public void processFirst(Scan s) {
      sum = s.getVal(fldname);
      count = 1;
      average = sum.asInt() / count;
   }

   /**
    * Add the field value in the current record to the sum.
    * Increment the count.
    * Recalculate the average.
    * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
    */
   public void processNext(Scan s) {
      Constant newval = s.getVal(fldname);
      sum.add(newval);
      count++;
      average = sum.asInt() / count;
   }

   /**
    * Return the field's name, prepended by "averageof".
    * @see simpledb.materialize.AggregationFn#fieldName()
    */
   public String fieldName() {
      return "averageof" + fldname;
   }

   /**
    * Return the current average.
    * @see simpledb.materialize.AggregationFn#value()
    */
   public Constant value() {
      return new Constant(average);
   }
}
