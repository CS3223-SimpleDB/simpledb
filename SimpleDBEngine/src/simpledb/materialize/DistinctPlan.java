package simpledb.materialize;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.Schema;

/**
 * The Plan class for the <i>distinct</i> operator.
 */
public class DistinctPlan implements Plan {
   private Plan p;
   private List<String> fieldlist;
   private Schema schema = new Schema();
   
   /**
    * Create a distinct plan for the underlying query.
    * @param tx the calling transaction
    * @param p a plan for the underlying query
    * @param fieldlist the list of fields
    */
   public DistinctPlan(Transaction tx, Plan p, List<String> fieldlist) {
      this.p = new SortPlan(tx, p, fieldlist);
      this.fieldlist = fieldlist;
      for (String fldname : fieldlist) {
         schema.add(fldname, p.schema());
      }
   }
   
   /**
    * Creates a distinct scan for this query.
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan s = p.open();
      return new DistinctScan(s, schema.fields());
   }

   /**
    * Estimates the number of block accesses in the operation,
    * which is the same as in the underlying query.
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
    * Estimates the number of distinct field values
    * in the operation,
    * which is the same as in the underlying query.
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }

   /**
    * Returns the schema of the operation,
    * which is taken from the field list.
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return schema;
   }

}
