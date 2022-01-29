package simpledb.test;
import java.util.Scanner;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class FindMajors {
   public static void main(String[] args) {
      System.out.print("Enter a department name: ");
      Scanner sc = new Scanner(System.in);
      String major = sc.next();
      sc.close();
      System.out.println("Here are the " + major + " majors");
      
      // analogous to the driver
      SimpleDB db = new SimpleDB("studentdb");

      // analogous to the connection
      Transaction tx  = db.newTx();
      Planner planner = db.planner();
      
      try {
         String qry = "select sname, gradyear "
                 + "from student, enroll "
                 + "where sid = studentid "
                 + "and eid >= 34";//'" + major + "'";
    	 Plan p = planner.createQueryPlan(qry, tx);
    	 Scan rs = p.open();
    	 System.out.println("Name\tGradYear");
         while (rs.next()) {
             String sname = rs.getString("sname");
             int gradyear = rs.getInt("gradyear");
             System.out.println(sname + "\t" + gradyear);
            //int dname = rs.getInt("sid");
            //System.out.println(dname);
            //System.out.println(sname);
         }
         
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}
