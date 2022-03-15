package simpledb.materialize;

import java.util.*;
import simpledb.query.*;

/**
 * The Scan class for the <i>aggregate</i> operator.
 */
public class AggregateScan implements Scan {
   private Scan s;
   private List<AggregationFn> aggfns;
   
   /**
    * Create an aggregate scan, given a table scan.
    * @param s the table scan
    * @param aggfns the aggregation functions
    */
   public AggregateScan(Scan s, List<AggregationFn> aggfns) {
      this.s = s;
      this.aggfns = aggfns;
   }
   
   /**
    * Position the scan before the table.
    * Internally, the underlying scan is always 
    * positioned at the first record of the table, which 
    * means that this method moves to the
    * first underlying record.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      s.beforeFirst();
   }

   /**
    * Move to the next tuple.
    * The aggregation functions are called for each record
    * in the table. 
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
      if (!s.next()) {
         return false;
      }
      for (AggregationFn fn : aggfns) {
         fn.processFirst(s);
      }
      while (s.next()) {
         for (AggregationFn fn : aggfns) {
            fn.processNext(s);
         }
      }
      return true;
   }

   /**
    * Get the integer value of the specified field.
    * The value is obtained from the
    * appropriate aggregation function.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public int getInt(String fldname) {
      return getVal(fldname).asInt();
   }

   /**
    * Get the string value of the specified field.
    * THe value is obtained from the
    * appropriate aggregation function.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public String getString(String fldname) {
      return getVal(fldname).asString();
   }

   /**
    * Get the Constant value of the specified field.
    * The value is obtained from the
    * appropriate aggregation function.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      for (AggregationFn fn : aggfns) {
         if (fn.fieldName().equals(fldname)) {
            return fn.value();
         }
      }
      throw new RuntimeException("field " + fldname + " not found.");
   }

   /**
    * Return true if the specified field is
    * created by an aggregation function.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      for (AggregationFn fn : aggfns) {
         if (fn.fieldName().equals(fldname)) {
            return true;
         }
      }
      return false;
   }

   /**
    * Close the scan by closing the underlying scan.
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      s.close();
   }

}
