package simpledb.multibuffer;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

public class BlockJoinScan implements Scan {
   private Transaction tx;
   private Scan leftScan, rightScan=null, nestedLoopScan;
   private String rightFilename, commonfield;
   private Layout rightLayout;
   private int chunkSize, nextBlockNumber, rightFilesize;
   
   
   /**
    * Creates the scan class for the product of the LHS scan and a table.
    * @param lhsscan the LHS scan
    * @param layout the metadata for the RHS table
    * @param tx the current transaction
    */
   public BlockJoinScan(Transaction tx, Scan leftScan, String rightTableName, Layout rightTableLayout, String commonfield) {
      this.tx = tx;
      this.leftScan = leftScan;
      this.rightFilename = rightTableName + ".tbl";
      this.rightLayout = rightTableLayout;
      this.commonfield = commonfield;
      rightFilesize = tx.size(rightFilename);
      int available = tx.availableBuffs();
      chunkSize = BufferNeeds.bestFactor(available, rightFilesize);
      beforeFirst();
   }
   
   /**
    * Positions the scan before the first record.
    * That is, the LHS scan is positioned at its first record,
    * and the RHS scan is positioned before the first record of the first chunk.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      nextBlockNumber = 0;
      useNextChunk();
   }
   
   /**
    * Moves to the next record in the current scan.
    * If there are no more records in the current chunk,
    * then move to the next LHS record and the beginning of that chunk.
    * If there are no more LHS records, then move to the next chunk
    * and begin again.
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
      while (!nestedLoopScan.next()) 
         if (!useNextChunk())
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
   
   private boolean useNextChunk() {
      if (nextBlockNumber >= rightFilesize)
         return false;
      if (rightScan != null)
         rightScan.close();
      int end = nextBlockNumber + chunkSize - 1;
      if (end >= rightFilesize)
         end = rightFilesize - 1;
      rightScan = new ChunkScan(tx, rightFilename, rightLayout, nextBlockNumber, end);
      leftScan.beforeFirst();
      nestedLoopScan = new NestedLoopJoinScan(leftScan, rightScan, commonfield);
      nextBlockNumber = end + 1;
      return true;
   }
}
