package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;

public class ChangeMajor {
   public static void main(String[] args) {
      try {
         SimpleDB db = new SimpleDB("studentdb");
         Transaction tx = db.newTx();
         Planner planner = db.planner();
       
         String cmd = "update STUDENT "
            + "set MajorId=30 "
            + "where SName = 'amy'";
         int numberOfAffectedRecords = planner.executeUpdate(cmd, tx);
         System.out.println("Amy is now a drama major.");
         System.out.println(Integer.valueOf(numberOfAffectedRecords) + " record(s) changed.");
         tx.commit();
      } catch(Exception e) {
         e.printStackTrace();
      }
   }
}
