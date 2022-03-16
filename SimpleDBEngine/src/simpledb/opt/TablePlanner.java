package simpledb.opt;

import java.util.ArrayList;
import java.util.Map;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.*;
import simpledb.index.planner.*;
import simpledb.materialize.MergeJoinPlan;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.multibuffer.BlockJoinPlan;
import simpledb.multibuffer.HashJoinPlan;
import simpledb.plan.*;

/**
 * This class contains methods for planning a single table.
 * @author Edward Sciore
 */
class TablePlanner {
   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private Map<String,IndexInfo> indexes;
   private Transaction tx;
   
   /**
    * Creates a new table planner.
    * The specified predicate applies to the entire query.
    * The table planner is responsible for determining
    * which portion of the predicate is useful to the table,
    * and when indexes are useful.
    * @param tblname the name of the table
    * @param mypred the query predicate
    * @param tx the calling transaction
    */
   public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
      this.mypred  = mypred;
      this.tx  = tx;
      myplan   = new TablePlan(tx, tblname, mdm);
      myschema = myplan.schema();
      indexes  = mdm.getIndexInfo(tblname, tx);
   }
   
   /**
    * Constructs a select plan for the table.
    * The plan will use an indexselect, if possible.
    * @return a select plan for the table.
    */
   public Plan makeSelectPlan() {
      Plan p = makeIndexSelect();
      if (p == null)
         p = myplan;
      return addSelectPred(p);
   }
   
   /**
    * Constructs a join plan of the specified plan and the table.
    * The plan will use the join with the lowest block accesses.
    * The plan will use the cross product if none of the given 
    * joins are suitable.
    * The method returns null if no join is possible.
    * @param current the specified plan
    * @return a join plan of the plan and this table
    */
   public Plan makeJoinPlan(Plan current) {
      Schema currsch = current.schema();
      Predicate joinpred = mypred.joinSubPred(myschema, currsch);
      if (joinpred == null) {
    	  return null;
      }
      ArrayList<Plan> plans = new ArrayList<>();
      //plans.add(makeIndexJoin(current, currsch));
      plans.add(makeSortJoin(current, currsch));
      //plans.add(makeNestedJoin(current, currsch));
     // plans.add(makeHashJoin(current, currsch));
      Plan cheapestPlan = lowestCostPlan(plans);
      if (cheapestPlan == null) {
         return makeProductJoin(current, currsch);
      }
      return cheapestPlan;
   }
   
   /**
    * Compares the block accesses of every join plan given.
    * Returns the join plan with the lowest block accesses.
    * Returns null otherwise.
    * @param plans the given list of join plans
    * @return the join plan with the lowest block accesses
    */
   private Plan lowestCostPlan(ArrayList<Plan> joinPlans) {
      Plan cheapestPlan = null;
      for (Plan currentPlan : joinPlans) {
         if (currentPlan == null) {
            continue;
         } else if (cheapestPlan == null || currentPlan.blocksAccessed() < cheapestPlan.blocksAccessed()) {
            cheapestPlan = currentPlan;
         }
      }
      return cheapestPlan;
   }
   
   private Plan addSelectPred(Plan p) {
      Predicate selectpred = mypred.selectSubPred(myschema);
      if (selectpred != null)
         return new SelectPlan(p, selectpred);
      else
         return p;
   }
   
   private Plan addJoinPred(Plan p, Schema currsch) {
      Predicate joinpred = mypred.joinSubPred(currsch, myschema);
      if (joinpred != null)
         return new SelectPlan(p, joinpred);
      else
         return p;
   }
   
   /**
    * Constructs a product plan of the specified plan and
    * this table.
    * @param current the specified plan
    * @return a product plan of the specified plan and this table
    */
   public Plan makeProductPlan(Plan current) {
      Plan p = addSelectPred(myplan);
      return new MultibufferProductPlan(tx, current, p);
   }
   
   private Plan makeProductJoin(Plan current, Schema currsch) {
      Plan p = makeProductPlan(current);
      return addJoinPred(p, currsch);
   }
   
   private Plan makeIndexSelect() {
      for (String fldname : indexes.keySet()) {
         Constant val = mypred.equatesWithConstantPlannerChecks(fldname);
         if (val != null) {
            IndexInfo ii = indexes.get(fldname);
            System.out.println("i am making index select");
            return new IndexSelectPlan(myplan, ii, val);
         }
      }
      return null;
   }
   
   private Plan makeIndexJoin(Plan current, Schema currsch) {
      for (String fldname : indexes.keySet()) {
    	 
         String outerfield = mypred.equatesWithFieldPlannerChecks(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
        	System.out.println("check which field matches");
        	System.out.println(fldname);
        	System.out.println(outerfield);
        	
            IndexInfo ii = indexes.get(fldname);
            Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
            p = addSelectPred(p);
            return addJoinPred(p, currsch);
         }
      }
      return null;
   }
   
   private Plan makeSortJoin(Plan current, Schema currsch) {
	   for (String fldname : myschema.fields()) {
		   String leftfield = mypred.equatesWithField(fldname);
		   if (leftfield != null && currsch.hasField(leftfield)) {
			   Plan p = new MergeJoinPlan(tx, current, myplan, leftfield, fldname);
			   p = addSelectPred(p);
			   return addJoinPred(p, currsch);
		   }
	   }
	   return null;
   }
   
   
   private Plan makeHashJoin(Plan current, Schema currsch) {
	   for (String fldname : myschema.fields()) {
		   // if the condition has a field = field condition
		   String leftfield = mypred.equatesWithFieldPlannerChecks(fldname);
		   if (leftfield != null && currsch.hasField(leftfield)) {
			   /* comment out this check for v1 of hash join, assuming no need recursive partition
		   	   if (tx.availableBuffs() < myplan.recordsOutput()) {
		   		   Plan p = new MultibufferProductPlan(tx, current, myplan);
		   		   p = addSelectPred(p);
		   		   return addJoinPred(p, currsch);
		   	   }
		   	   */
		   	   // since size of T2 > available buffer, create hash join plan
			   Plan p = new HashJoinPlan(tx, current, myplan, leftfield, fldname);
			   p = addSelectPred(p);
			   return addJoinPred(p, currsch);
		   }
	   }
	   return null;
   }
   
   /**
    * Checks for common join fields between the two joining schemas.
    * Constructs a new BlockJoinPlan if at least one common join field is found.
    * Returns null if there are no common join fields.
    * @param current the specified plan
    * @param currsch the schema of the specified plan
    * @return the join predicate of the newly constructed BlockJoinPlan
    */
   private Plan makeNestedJoin(Plan current, Schema currsch) {
      for (String fldname : myschema.fields()) {
         String commonfield = mypred.equatesWithField(fldname);
         if (commonfield != null && currsch.hasField(commonfield)) {
            Plan p = new BlockJoinPlan(tx, current, myplan, commonfield);
            p = addSelectPred(p);
            return addJoinPred(p, currsch);
         }
      }
      return null;
   }
}