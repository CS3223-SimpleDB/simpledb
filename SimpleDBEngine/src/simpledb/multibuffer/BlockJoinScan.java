package simpledb.multibuffer;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

/** 
 * The Scan class for the <i>block nested loops join</i> operator.
 */
public class BlockJoinScan implements Scan {
   private Transaction tx;
   private Scan leftScan, rightScan=null, nestedLoopScan;
   private String rightFilename, lhsfield, rhsfield, oprType;
   private Layout rightLayout;
   private int blockSize, nextBlockNumber, rightFilesize;
   
   
   /**
    * Creates the scan class for the block nested loops join of the LHS scan and a table.
    * @param tx the current transaction
    * @param leftScan the LHS scan
    * @param rightTableName the name of the RHS table
    * @param rightTableLayout the metadata for the RHS table
    * @param commonfield the common joining field between both plans
    */
   public BlockJoinScan(Transaction tx, Scan leftScan, String rightTableName,
		   Layout rightTableLayout, String lhsfield, String rhsfield, String oprType) {
      this.tx = tx;
      this.leftScan = leftScan;
      this.rightFilename = rightTableName + ".tbl";
      this.rightLayout = rightTableLayout;
      this.lhsfield = lhsfield;
      this.rhsfield = rhsfield;
      this.oprType = oprType;
      rightFilesize = tx.size(rightFilename);
      int available = tx.availableBuffs();
      blockSize = BufferNeeds.bestFactor(available, rightFilesize);
      beforeFirst();
   }
   
   /**
    * Positions the LHS and RHS scans before their first records.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      nextBlockNumber = 0;
      useNextBlock();
   }
   
   /**
    * Moves to the next record in the current scan.
    * If there are no more records in the current block,
    * then move to the next LHS record and the beginning of that block.
    * If there are no more LHS records, then move to the next block
    * and begin again.
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
      while (!nestedLoopScan.next()) 
         if (!useNextBlock())
         return false;
      return true;
   }
   
   /**
    * Closes the current scans.
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      nestedLoopScan.close();
   }
   
   /** 
    * Returns the value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      return nestedLoopScan.getVal(fldname);
   }
   
   /** 
    * Returns the integer value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public int getInt(String fldname) {
      return nestedLoopScan.getInt(fldname);
   }
   
   /** 
    * Returns the string value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public String getString(String fldname) {
      return nestedLoopScan.getString(fldname);
   }
   
   /**
    * Returns true if the specified field is in
    * either of the underlying scans.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return nestedLoopScan.hasField(fldname);
   }
   
   /**
    * Retrieves the next block for scanning and joining if possible.
    * Returns true if there is a next block and false otherwise.
    * @return
    */
   private boolean useNextBlock() {
      if (nextBlockNumber >= rightFilesize)
         return false;
      if (rightScan != null)
         rightScan.close();
      int end = nextBlockNumber + blockSize - 1;
      if (end >= rightFilesize)
         end = rightFilesize - 1;
      rightScan = new BlockScan(tx, rightFilename, rightLayout, nextBlockNumber, end);
      leftScan.beforeFirst();
      nestedLoopScan = new NestedLoopsJoinScan(leftScan, rightScan, lhsfield, rhsfield, oprType);
      nextBlockNumber = end + 1;
      return true;
   }
}
