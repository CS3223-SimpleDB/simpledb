package simpledb.materialize;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.Schema;

/**
 * The Plan class for the <i>aggregate</i> operator.
 */
public class AggregatePlan implements Plan {
   private Plan p;
   private List<AggregationFn> aggfns;
   private Schema schema = new Schema();
   
   /**
    * Create an aggregate plan for the underlying query.
    * The aggregation is computed by the
    * specified collection of aggregation functions.
    * @param tx the calling transaction
    * @param p a plan for the underlying query
    * @param aggfns the aggregation functions
    */
   public AggregatePlan(Transaction tx, Plan p, List<AggregationFn> aggfns) {
      this.p = p;
      this.aggfns = aggfns;
      for (AggregationFn fn : aggfns) {
         schema.addIntField(fn.fieldName());
      }
   }
   
   /**
    * This method opens a project plan for the specified plan.
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan s = p.open();
      return new AggregateScan(s, aggfns);
   }

   /**
    * Return the number of blocks required to
    * compute the aggregation,
    * which is one pass through the table.
    * It does <i>not</i> include the one-time cost
    * of materializing the records.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return p.blocksAccessed();
   }

   /**
    * Estimates the number of output records in the operation,
    * which is the same as in the underlying query.
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }

   /**
    * Return the number of distinct values for the
    * specified field.
    * All values are assumed to be distinct.
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }

   /**
    * Returns the schema of the output table.
    * The schema consists of one field for each aggregation function.
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return schema;
   }

}
