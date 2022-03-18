package simpledb.materialize;

import java.util.*;
import simpledb.query.*;

/**
 * The Scan class for the <i>distinct</i> operator.
 */
public class DistinctScan implements Scan {
   private Scan s;
   private List<String> fieldlist;
   private GroupValue groupval;
   private boolean moregroups;
   
   /**
    * Create a distinct scan given the scan of a DistinctPlan
    * and list of selected fields.
    * @param s the underlying scan
    * @param fieldlist the list of field names
    */
   public DistinctScan(Scan s, List<String> fieldlist) {
      this.s = s;
      this.fieldlist = fieldlist;
      beforeFirst();
   }
   
   /**
    * Position the scan before the first group.
    * Internally, the underlying scan is always 
    * positioned at the first record of a group, which 
    * means that this method moves to the
    * first underlying record.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      s.beforeFirst();
      moregroups = s.next();
   }
   
   /**
    * Move to the next group.
    * The key of the group is determined by the 
    * group values at the current record.
    * The method repeatedly reads underlying records until
    * it encounters a record having a different key.
    * The values of the grouping fields for the group are saved.
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
      if (!moregroups) {
         return false;
      }
      groupval = new GroupValue(s, fieldlist);
      while (moregroups = s.next()) {
         GroupValue gv = new GroupValue(s, fieldlist);
         if (!groupval.equals(gv)) {
            break;
         }
      }
      return true;
   }
   
   /**
    * Get the integer value of the specified field.
    * from the saved group value.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public int getInt(String fldname) {
      return getVal(fldname).asInt();
   }
   
   /**
    * Get the string value of the specified field.
    * from the saved group value.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public String getString(String fldname) {
      return getVal(fldname).asString();
   }
   
   /**
    * Get the Constant value of the specified field
    * from the saved group value.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      if (fieldlist.contains(fldname)) {
         return groupval.getVal(fldname);
      } else {
         throw new RuntimeException("field " + fldname + " not found.");
      }
   }
   
   /** 
    * Checks if the specified field is in 
    * the list of selected fields of the current query.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      if (fieldlist.contains(fldname)) {
         return true;
      } else {
         return false;
      }
   }
   
   /**
    * Close the scan by closing the underlying scan.
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      s.close();
   }

}
