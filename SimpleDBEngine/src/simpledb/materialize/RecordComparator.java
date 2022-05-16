package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

/**
 * A comparator for scans.
 * @author Edward Sciore
 */
public class RecordComparator implements Comparator<Scan> {
   private List<String> fields;
   private List<String> directions;
   
   /**
    * Create a comparator using the specified fields,
    * using the ordering implied by its iterator.
    * @param fields a list of field names
    */
   public RecordComparator(List<String> fields) {
       this.fields = fields;
       this.directions = new LinkedList<String>();
   }
   
   public RecordComparator(List<String> fields, List<String> directions) {
       this.fields = fields;
       this.directions = directions;
   }
   
   /**
    * Compare the current records of the two specified scans.
    * The sort fields are considered in turn.
    * When a field is encountered for which the records have
    * different values, those values are used as the result
    * of the comparison.
    * If the two records have the same values for all
    * sort fields, then the method returns 0.
    * @param s1 the first scan
    * @param s2 the second scan
    * @return the result of comparing each scan's current record according to the field list
    */
   public int compare(Scan s1, Scan s2) {
      for (String fldname : fields) {
         Constant val1 = s1.getVal(fldname);
         Constant val2 = s2.getVal(fldname);
         int result = val1.compareTo(val2);
         if (result != 0)
            return result;
      }
      return 0;
   }
   
   public List<String> compareSort(Scan s1, Scan s2) {
	  List<String> resultList = new LinkedList<String>();
      for (String fldname : fields) {
         Constant val1 = s1.getVal(fldname);
         Constant val2 = s2.getVal(fldname);
         int result = val1.compareTo(val2);
         if (result != 0) {
        	 int fldnameIndex = fields.indexOf(fldname);
        	 if (directions.size() != 0) {
            	 String direction = directions.get(fldnameIndex);
            	 resultList.add(direction);
        	 } else {
        		 resultList.add("random");
        	 }
        	 resultList.add(String.valueOf(result));
        	 return resultList;
         }
      }
      return resultList;
   }
   
   public boolean isDirectionsPresent() {
	   return (directions.size() != 0);
   }
}
