package simpledb.multibuffer;

import java.util.HashMap;
import java.util.List;

import simpledb.materialize.RecordComparator;
import simpledb.materialize.TempTable;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;

public class HashJoinScan implements Scan {
	private Scan rhs;
	private Scan tempScan;
	private HashMap<Integer, HashPartition> rhsPartitions;
	private String fldname1, fldname2;
	private int hashTableSize;
	
	/**
	 * e.g. did = majorid, LHS field -> did, RHS field -> majorid
	 * @param s1 LHS scan
	 * @param s2 RHS scan
	 * @param comp comparator
	 * @param fldname1 LHS field
	 * @param fldname2 RHS field
	 */
	public HashJoinScan(Scan rhs, HashMap<Integer, HashPartition> rhsPartitions,
			String fldname1, String fldname2, int hashTableSize) {
		this.rhs = rhs;
		this.rhsPartitions = rhsPartitions;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.hashTableSize = hashTableSize;
	}
	
	/**
	  * Position the scan before the first record in sorted order.
	  * Internally, it moves to the first record of each underlying scan.
	  * The variable currentscan is set to null, indicating that there is
	  * no current scan.
	  * @see simpledb.query.Scan#beforeFirst()
	  */
	public void beforeFirst() {
		// Position RHS probe to be before first record
		rhs.beforeFirst();
	}
	   
	/**
	  * Return true if the specified field is in the current scan.
	  * @see simpledb.query.Scan#hasField(java.lang.String)
	  */
	public boolean hasField(String fldname) {
		return rhs.hasField(fldname) || tempScan.hasField(fldname);
	}

	public boolean next() {

		// Step 1: check if the partition from current scan has been looped finish
		if (tempScan != null) {
			while(tempScan.next()) {
				// get partition val
				Constant partitionVal = tempScan.getVal(fldname1);
				// get current scan value
				Constant value = rhs.getVal(fldname2);
				if (partitionVal.equals(value)) {
					return true;
				}
			}
			tempScan.close();
		}
		// Step 2: call next function of the probe function
		boolean hasmore = rhs.next();
		while (hasmore) {
			// Step 2: process record to get the value of the field to join
			Constant value = rhs.getVal(fldname2);
			// Step 3: process value to obtain partition to match
			int hashCodeVal = value.hashCode();
			int modVal = hashCodeVal % hashTableSize;
			
			// Step 4: Try to retrieve temporary table of the partition
			if (rhsPartitions.get(modVal) != null) {
				HashPartition currentPartition = rhsPartitions.get(modVal);
				UpdateScan partitionScan = currentPartition.open();
				partitionScan.beforeFirst();
				tempScan = partitionScan;
				// Step 5: Loop through all records in partition to match
				while(partitionScan.next()) {
					Constant partitionVal = partitionScan.getVal(fldname1);
					if (value.equals(partitionVal)) {
						return true;
					}
				}
				tempScan.close();
			}
			hasmore = rhs.next();
		}
		return false;
	}

	public int getInt(String fldname) {
	    if (rhs.hasField(fldname))
	        return rhs.getInt(fldname);
	    else
	        return tempScan.getInt(fldname);
	}

	@Override
	public String getString(String fldname) {
	    if (rhs.hasField(fldname))
	        return rhs.getString(fldname);
	    else
	        return tempScan.getString(fldname);
	}

	public Constant getVal(String fldname) {
	    if (rhs.hasField(fldname))
	        return rhs.getVal(fldname);
	    else
	        return tempScan.getVal(fldname);
	}

	
	public void close() {
		// close the probe table and temp table
		rhs.close();
	}
}
