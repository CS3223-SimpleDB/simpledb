package simpledb.multibuffer;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.materialize.*;
import simpledb.plan.Plan;

/**
 * The Plan class for the <i>block nested loops join</i> operator.
 */
public class BlockJoinPlan implements Plan {
   private Transaction tx;
   private Plan lhs, rhs;
   private String commonfield;
   private Schema schema = new Schema();

   /**
    * Creates a block nested loops join plan for the specified queries.
    * @param tx the calling transaction
    * @param lhs the plan for the LHS query
    * @param rhs the plan for the RHS query
    * @param commonfield the common joining field between both plans
    */
   public BlockJoinPlan(Transaction tx, Plan lhs, Plan rhs, String commonfield) {
      this.tx = tx;
      this.lhs = new MaterializePlan(tx, lhs);
      this.rhs = rhs;
      this.commonfield = commonfield;
      schema.addAll(lhs.schema());
      schema.addAll(rhs.schema());
   }

   /**
    * A scan for this query is created and returned, as follows.
    * First, the method materializes its LHS and RHS queries.
    * It then determines the optimal block size,
    * based on the size of the materialized RHS file and the
    * number of available buffers.
    * It creates a block plan for each block, saving them in a list.
    * Finally, it creates a BlockJoinScan for this list of plans,
    * and returns that scan.
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan leftScan = lhs.open();
      TempTable tempRightTable = copyRecordsFrom(rhs);
      return new BlockJoinScan(tx, leftScan, tempRightTable.tableName(), tempRightTable.getLayout(), commonfield);
   }

   /**
    * Returns an estimate of the number of block accesses
    * required to execute the query. The formula is:
    * <pre> B(blockNestedLoopsJoin(p1,p2)) = B(p2) + B(p1)*C(p2) </pre>
    * where C(p2) is the number of blocks of p2.
    * The method uses the current number of available buffers
    * to calculate C(p2), and so this value may differ
    * when the query scan is opened.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      // this guesses at the # of blocks
      int avail = tx.availableBuffs(); //number of buffers
      int size = new MaterializePlan(tx, rhs).blocksAccessed(); //number of accesses (IOs)
      int numberOfBlocks = size / avail; //number of blocks
      return rhs.blocksAccessed() +
            (lhs.blocksAccessed() * numberOfBlocks); //cost (IOs)
   }

   /**
    * Estimates the number of output records in the block nested loops join.
    * The formula is:
    * <pre> R(blockNestedLoopsJoin(p1,p2)) = R(p1)*R(p2) </pre>
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return lhs.recordsOutput() * rhs.recordsOutput();
   }

   /**
    * Estimates the distinct number of field values in the block nested loops join.
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      if (lhs.schema().hasField(fldname))
         return lhs.distinctValues(fldname);
      else
         return rhs.distinctValues(fldname);
   }

   /**
    * Returns the schema of the block nested loops join,
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return schema;
   }

   /**
    * Copies all fields and their values of the specified plan to a temporary table.
    * @param p the specified plan
    * @return the copy of the specified plan as a temporary table
    */
   private TempTable copyRecordsFrom(Plan p) {
      Scan   src = p.open(); 
      Schema sch = p.schema();
      TempTable t = new TempTable(tx, sch);
      UpdateScan dest = (UpdateScan) t.open();
      while (src.next()) {
         dest.insert();
         for (String fldname : sch.fields())
            dest.setVal(fldname, src.getVal(fldname));
      }
      src.close();
      dest.close();
      return t;
   }
}
