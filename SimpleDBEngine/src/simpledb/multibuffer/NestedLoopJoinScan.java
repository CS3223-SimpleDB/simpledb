package simpledb.multibuffer;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class NestedLoopJoinScan implements Scan {
   private Scan lhs, rhs;
   private String commonfield;

   /**
    * Create a product scan having the two underlying scans.
    * @param s1 the LHS scan
    * @param s2 the RHS scan
    */
   public NestedLoopJoinScan(Scan lhs, Scan rhs, String commonfield) {
      this.lhs = lhs;
      this.rhs = rhs;
      this.commonfield = commonfield;
      beforeFirst();
   }

   /**
    * Position the scan before its first record.
    * In particular, the LHS scan is positioned at 
    * its first record, and the RHS scan
    * is positioned before its first record.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      lhs.beforeFirst();
      lhs.next();
      rhs.beforeFirst();
   }

   /**
    * Move the scan to the next record.
    * The method moves to the next RHS record, if possible.
    * Otherwise, it moves to the next LHS record and the
    * first RHS record.
    * If there are no more LHS records, the method returns false.
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
      while (rhs.next()) {
         if (rhs.getVal(commonfield).equals(lhs.getVal(commonfield))) {
            return true;
         }
      }
      rhs.beforeFirst();
      return rhs.next() && lhs.next();
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
