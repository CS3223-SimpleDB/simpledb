package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;

public class ChangeMajor {
   public static void main(String[] args) {
      try {
          // analogous to the driver
          SimpleDB db = new SimpleDB("studentdb");

          // analogous to the connection
          Transaction tx  = db.newTx();
          Planner planner = db.planner();
          
          // analogous to the statement
          String qry = "update STUDENT "
                  + "set MajorId=30 "
                  + "where SName = 'amy'";
          int numberOfAffectedRecords = planner.executeUpdate(qry, tx);
          
          // analogous to the result set
          System.out.println("Amy is now a drama major.");
          System.out.println(Integer.valueOf(numberOfAffectedRecords) + " record(s) changed.");
          tx.commit();
       }
       catch(Exception e) {
          e.printStackTrace();
       }
   }
}
