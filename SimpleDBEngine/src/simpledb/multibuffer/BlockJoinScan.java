package simpledb.multibuffer;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.tx.Transaction;

public class BlockJoinScan implements Scan {
   private Transaction tx;
   private Scan lhsscan, rhsscan=null, nestedLoopScan;
   private String filename, commonfield;
   private Layout layout;
   private int chunksize, nextblknum, filesize;
   
   
   /**
    * Creates the scan class for the product of the LHS scan and a table.
    * @param lhsscan the LHS scan
    * @param layout the metadata for the RHS table
    * @param tx the current transaction
    */
   public BlockJoinScan(Transaction tx, Scan lhsscan, String tblname, Layout layout, String commonfield) {
      this.tx = tx;
      this.lhsscan = lhsscan;
      this.filename = tblname + ".tbl";
      this.layout = layout;
      this.commonfield = commonfield;
      filesize = tx.size(filename);
      int available = tx.availableBuffs();
      chunksize = BufferNeeds.bestFactor(available, filesize);
      beforeFirst();
   }
   
   /**
    * Positions the scan before the first record.
    * That is, the LHS scan is positioned at its first record,
    * and the RHS scan is positioned before the first record of the first chunk.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      nextblknum = 0;
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
      if (nextblknum >= filesize)
         return false;
      if (rhsscan != null)
         rhsscan.close();
      int end = nextblknum + chunksize - 1;
      if (end >= filesize)
         end = filesize - 1;
      rhsscan = new ChunkScan(tx, filename, layout, nextblknum, end);
      lhsscan.beforeFirst();
      nestedLoopScan = new NestedLoopJoinScan(lhsscan, rhsscan, commonfield);
      nextblknum = end + 1;
      return true;
   }
}