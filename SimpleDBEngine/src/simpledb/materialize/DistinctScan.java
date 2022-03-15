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
    * Create a distinct scan having the specified
    * underlying scan and field list.
    * @param s the underlying scan
    * @param fieldlist the list of field names
    */
   public DistinctScan(Scan s, List<String> fieldlist) {
      this.s = s;
      this.fieldlist = fieldlist;
      beforeFirst();
   }
   
   public void beforeFirst() {
      s.beforeFirst();
      moregroups = s.next();
   }

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

   public int getInt(String fldname) {
      return getVal(fldname).asInt();
   }

   public String getString(String fldname) {
      return getVal(fldname).asString();
   }

   public Constant getVal(String fldname) {
      if (fieldlist.contains(fldname)) {
         return groupval.getVal(fldname);
      } else {
         throw new RuntimeException("field " + fldname + " not found.");
      }
   }

   public boolean hasField(String fldname) {
      if (fieldlist.contains(fldname)) {
         return true;
      } else {
         return false;
      }
   }

   public void close() {
      s.close();
   }

}
