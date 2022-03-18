package simpledb.multibuffer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import simpledb.materialize.MaterializePlan;
import simpledb.materialize.TempTable;
import simpledb.plan.Plan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 * The Plan class for the hash join version of the
 * join.
 */

public class HashJoinPlan implements Plan {
	   private Transaction tx;
	   private Plan lhs, rhs;
	   private Schema lhsSchema = new Schema();
	   private Schema rhsSchema = new Schema();
	   private Schema schema = new Schema();
	   private String joinFieldLhs, joinFieldRhs;

	   /**
	    * Creates a hash join plan for the specified queries.
	    * @param lhs the plan for the LHS query
	    * @param rhs the plan for the RHS query
	    * @param tx the calling transaction
	    */
	   public HashJoinPlan(Transaction tx, Plan lhs, Plan rhs, String joinFieldLhs, String joinFieldRhs) {
	      this.tx = tx;
	      this.lhs = lhs;
	      this.rhs = rhs;
	      this.joinFieldLhs = joinFieldLhs;
	      this.joinFieldRhs = joinFieldRhs;
	      this.lhsSchema.addAll(lhs.schema());
	      this.rhsSchema.addAll(rhs.schema());
	      this.schema.addAll(lhs.schema());
	      this.schema.addAll(rhs.schema());
	   }
	   
	   public Scan open() {
		  // see whether lhs/rhs can handle the current buffer choose lhs/rhs to be partitioning table
		  int availBuff = tx.availableBuffs();
		  System.out.println(availBuff);
		  int hashTableSize = availBuff - 1;
		  
		  // stores a list of integer and scans
		  HashMap<Integer, HashPartition> hashTable =
				  new HashMap<Integer, HashPartition>(hashTableSize, (float) 1.0);
		  
		  int hashMod = hashTableSize;
		  
		  hashTable = buildHashTable(hashTable, hashTableSize);
		  
	      Scan rhsScan = rhs.open();
		  return new HashJoinScan(rhsScan, hashTable, joinFieldLhs, joinFieldRhs, hashTableSize);
	   }
	   
	   private HashMap<Integer, HashPartition> buildHashTable(HashMap<Integer,
			   HashPartition> hashTable,int hashTableSize) {
		   
		   // Each partition, 1 hash map, to store the corresponding field -> values
		   List<HashMap<String, List<Constant>>> finalResult =
				   new LinkedList<HashMap<String, List<Constant>>>();
		   
		   for (int i = 0; i < hashTableSize; i++) {
			   finalResult.add(new HashMap<String, List<Constant>>());
		   }
		   
		   int numOfFields = lhsSchema.fields().size();
		   
		   // Step 1: Open LHS scan
		   Scan leftScan = lhs.open();
		   leftScan.beforeFirst();
		   boolean leftScanHasRecord = leftScan.next();
		   while (leftScanHasRecord) {
			   // Step 2: For each record, partition them into modVal, where partitions is mod (k-1)
			   Constant val = leftScan.getVal(joinFieldLhs);
			   int hashCodeVal = val.hashCode();
			   int modVal = hashCodeVal % hashTableSize;
			   HashMap<String, List<Constant>> partitionMap = finalResult.get(modVal);
			   
			   if (partitionMap.get(lhsSchema.fields().get(0)) != null) {
				   // some elements are inserted into partition already
				   for (String fldname : lhsSchema.fields()) {
					   Constant fieldVal = leftScan.getVal(fldname);
					   List<Constant> fieldResults = partitionMap.get(fldname);
					   fieldResults.add(fieldVal);
				   }
				   leftScanHasRecord = leftScan.next();
			   } else {
				   // first insertion into this partition;
				   for (String fldname : lhsSchema.fields()) {
					   Constant fieldVal = leftScan.getVal(fldname);
					   List<Constant> fieldResults = new LinkedList<Constant>();
					   fieldResults.add(fieldVal);
					   partitionMap.put(fldname, fieldResults);
				   }
				   leftScanHasRecord = leftScan.next();
			   }
			   
			   /*
			   // Step 3: insert into partition
			   if (hashTable.get(modVal) != null) {
				   // partition already created
				   HashPartition currentPartition = hashTable.get(modVal);
				   UpdateScan partitionScan = currentPartition.open();
				   
				   //Step 4: insert new record into partition and close scan
				   leftScanHasRecord = copy(leftScan, partitionScan);
				   partitionScan.close();
			   } else {
				   // Step 4: Create new TempTable for current partition
				   HashPartition newPartition = new HashPartition(tx, lhsSchema);
				   UpdateScan partitionScan = newPartition.open();
				   
				   // Step 5: insert new record into partition and close scan;
				   leftScanHasRecord = copy(leftScan, partitionScan);
				   partitionScan.close();
				   
				   //Step 6: Update hash table
				   hashTable.put(modVal, newPartition);
			   }*/
			   
		   }
		   
		   // Step 7: write all partitions to disc:
		   copy(finalResult, hashTable);
		   
		   // Last Step: Close scan
		   leftScan.close();
		   return hashTable;
	   }
	   
	   private boolean copy(List<HashMap<String, List<Constant>>> results,
			   HashMap<Integer, HashPartition>  hashTable) {
		   
		   int counter = 0;
		   for (HashMap<String, List<Constant>> partition : results) {
			   if (partition.get(lhsSchema.fields().get(0)) != null) {
				   // partition exists, start to copy
				   HashPartition newPartition = new HashPartition(tx, lhsSchema);
				   UpdateScan partitionScan = newPartition.open();
				   
				   int valuesToAdd = partition.get(lhsSchema.fields().get(0)).size();
				   for (int i = 0; i < valuesToAdd; i++) {
					   partitionScan.insert();
					   for (String fldname : lhsSchema.fields()) {
						   Constant val = partition.get(fldname).get(i);
						   partitionScan.setVal(fldname, val);
					   }
				   }
				   
				   partitionScan.close();
				   hashTable.put(counter, newPartition);
			   }
			   counter++;
		   }
		   return true;
	   }
	   
	   private boolean copy (Scan src, UpdateScan dest) {
	       dest.insert();
		   for (String fldname : lhsSchema.fields())
		       dest.setVal(fldname, src.getVal(fldname));
		   return src.next();
	   }
	   
	   /**
	    * Returns an estimate of the number of block accesses
	    * required to execute the query. The formula is:
	    * <pre> B(join(p1,p2)) = B(p1) + V(per partition)*R(p2) </pre>
	    * where C(p2) is the number of blocks of p2.
	    * @see simpledb.plan.Plan#blocksAccessed()
	    */
	   public int blocksAccessed() {
		  // cost: lhs block accessed once during partitioning phase. During matching phase,
		  // each tuple will scan through all blocks in lhs partition
		  int availableBuffer = tx.availableBuffs();
		  int numTuplesEachPartition = Math.round(lhs.recordsOutput() / availableBuffer);
	      return lhs.blocksAccessed() +
	            (numTuplesEachPartition * rhs.recordsOutput()); //cost (IOs)
	   }

	   /**
	    * Estimates the number of output records in the product.
	    * The formula is:
	    * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
	    * @see simpledb.plan.Plan#recordsOutput()
	    */
	   public int recordsOutput() {
		      int maxvals = Math.max(lhs.distinctValues(joinFieldLhs), rhs.distinctValues(joinFieldRhs));
		      return (lhs.recordsOutput() * rhs.recordsOutput()) / maxvals;
	   }

	   /**
	    * Estimates the distinct number of field values in the product.
	    * Since the product does not increase or decrease field values,
	    * the estimate is the same as in the appropriate underlying query.
	    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
	    */
	   public int distinctValues(String fldname) {
	      if (lhs.schema().hasField(fldname))
	         return lhs.distinctValues(fldname);
	      else
	         return rhs.distinctValues(fldname);
	   }

	   /**
	    * Returns the lhs schema of the plan,
	    * @see simpledb.plan.Plan#schema()
	    */
	   public Schema lhsSchema() {
	       return lhsSchema;
	   }
	   
	   public Schema rhsSchema() {
		   return rhsSchema;
	   }
	   
	   public Schema schema() {
		  return schema;
	   }
	   
	   public String toString() {
		   return "[" + lhs.toString() + " hash join " + rhs.toString() + "]";
	   }
}
