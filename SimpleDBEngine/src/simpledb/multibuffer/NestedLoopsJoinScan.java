package simpledb.multibuffer;

import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The scan class corresponding to the <i>nested loops join</i> 
 * relational algebra operator.
 */
public class NestedLoopsJoinScan implements Scan {
   private Scan lhs, rhs;
   private String lhsfield, rhsfield;
   private boolean isContinue = false;

   /**
    * Create a nested loop join scan having the two underlying scans.
    * @param s1 the LHS scan
    * @param s2 the RHS scan
    * @param commonfield the common joining field between both scans
    */
   public NestedLoopsJoinScan(Scan lhs, Scan rhs, String lhsfield, String rhsfield) {
      this.lhs = lhs;
      this.rhs = rhs;
      this.lhsfield = lhsfield;
      this.rhsfield = rhsfield;
      beforeFirst();
   }

   /**
    * Positions the LHS and RHS scans before their first records.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      lhs.beforeFirst();
      rhs.beforeFirst();
   }

   /**
    * For each LHS record, checks through all RHS records to find the
    * first one that has the same value as the current LHS record.
    * If there is a matching pair, returns true.
    * If there are no more RHS records, moves the RHS scan back to before
    * its first record and the LHS scan to its next record.
    * If there are no more LHS records, returns false.
    * @return a boolean indicating the success of a match
    */
   public boolean next() {
	  if (isContinue) {
		  while(rhs.next()) {
	            if (lhs.getVal(lhsfield).equals(rhs.getVal(rhsfield))) {
	                return true;
	            }
		  }
		  isContinue = false;
		  rhs.beforeFirst();
	  }
	  
      while (lhs.next()) {
         while (rhs.next()) {
            if (lhs.getVal(lhsfield).equals(rhs.getVal(rhsfield))) {
               isContinue = true;
               return true;
            }
         }
         rhs.beforeFirst();
      }
      return false;
   }

   /** 
    * Return the integer value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public int getInt(String fldname) {
      if (lhs.hasField(fldname))
         return lhs.getInt(fldname);
      else
         return rhs.getInt(fldname);
   }

   /** 
    * Returns the string value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public String getString(String fldname) {
      if (lhs.hasField(fldname))
         return lhs.getString(fldname);
      else
         return rhs.getString(fldname);
   }

   /** 
    * Return the value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      if (lhs.hasField(fldname))
         return lhs.getVal(fldname);
      else
         return rhs.getVal(fldname);
   }

   /**
    * Returns true if the specified field is in
    * either of the underlying scans.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return lhs.hasField(fldname) || rhs.hasField(fldname);
   }

   /**
    * Close both underlying scans.
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      lhs.close();
      rhs.close();
   }
}
