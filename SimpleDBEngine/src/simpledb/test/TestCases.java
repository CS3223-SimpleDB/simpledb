package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.*;
import simpledb.server.SimpleDB;

public class TestCases {
   public static final String select1 = "select sid, sname, gradyear, majorid from student";
   public static final String nonequi1 = "select sid, sname, gradyear, majorid from student where sid < 5";
   public static final String nonequi2 = "select sid, sname, gradyear, majorid from student where sid > 5";
   public static final String nonequi3 = "select sid, sname, gradyear, majorid from student where sid <= 5";
   public static final String nonequi4 = "select sid, sname, gradyear, majorid from student where sid >= 5";
   public static final String nonequi5 = "select sid, sname, gradyear, majorid from student where sid != 5";
   public static final String nonequi6 = "select sid, sname, gradyear, majorid from student where sid <> 5";
   public static final String order1 = "select sid, sname, gradyear, majorid from student order by gradyear";
   public static final String order2 = "select sid, sname, gradyear, majorid from student order by gradyear asc";
   public static final String order3 = "select sid, sname, gradyear, majorid from student order by gradyear desc";
   public static final String order4 = "select sid, sname, gradyear, majorid from student order by gradyear, sid";
   public static final String order5 = "select sid, sname, gradyear, majorid from student order by gradyear, sid desc";
   public static final String distinct1 = "select distinct sid, sname, gradyear, majorid from student";
   public static final String distinct2p1 = "select gradyear, majorid from student order by gradyear";
   public static final String distinct2p2 = "select distinct gradyear, majorid from student";
   public static final String agg1 = "select sid, sname, avg(gradyear), majorid from student";
   public static final String agg2 = "select sid, count(sname), gradyear, majorid from student";
   public static final String agg3 = "select sid, sname, max(gradyear), majorid from student";
   public static final String agg4 = "select sid, sname, min(gradyear), majorid from student";
   public static final String agg5 = "select sum(sid), sname, gradyear, majorid from student";
   //public static final String nestedjoin = "select dname, did, cid, title from course, dept where did!=cid order by did, cid desc";
   public static final String nestedjoin = "select sid, sname, gradyear, dname from student, dept where did = majorid and gradyear!=2022 order by gradyear desc, sid";
   public static final String grp1 = "select gradyear from student group by gradyear";
   public static final String grp2 = "select count(sname), gradyear from student group by gradyear";
   public static final String grp3 = "select min(gradyear), majorid from student group by majorid";
   public static final String grp4 = "select max(gradyear), majorid from student group by majorid";
   public static final String grp5 = "select sum(sid), majorid from student group by majorid";
   public static final String grp6 = "select avg(sid), majorid from student group by majorid";
   
   public static void main(String[] args) {		
      try {
         SimpleDB db = new SimpleDB("studentdb");
         Transaction tx  = db.newTx(); 
         Planner planner = db.planner();
         //QUERY
         String qry = select1;
         
         Plan p = planner.createQueryPlan(qry, tx);
         Scan s = p.open();
         //HEADER
         System.out.println("SID\tNAME\tGYEAR\tMAJORID");
         //System.out.println("AVG\tMAJORID");
         
         //AGGREGATE (avgof, countof, maxof, minof, sumof) *uncomment this block, comment out while loop below
         /*s.next();
         int agg = s.getInt("sumofsid");
         System.out.println(agg);*/
         
         while (s.next()) {
            //READ VALUES
            //int avgofsid = s.getInt("avgofsid");
            int sid = s.getInt("sid");
            String sname = s.getString("sname");
            int gradyear = s.getInt("gradyear");
            int majorid = s.getInt("majorid");
            //String majorid = s.getString("dname");
            //int majorid = s.getInt("did");
            //String dname = s.getString("dname");
            //int did = s.getInt("did");
            //int cid = s.getInt("cid");
            //String title = s.getString("title");
            
            //PRINT VALUES
            System.out.println(sid + "\t" + sname + "\t" + gradyear + "\t" + majorid);
            //System.out.println(dname + "\t" + did + "\t" + cid + "\t" + title);
            //System.out.println(avgofsid + "\t" + majorid);
         }
         
         s.close();
         tx.commit();
      } catch(Exception e) {
          e.printStackTrace();    
      }   
   }   
}
