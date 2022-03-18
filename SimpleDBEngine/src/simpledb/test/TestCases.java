package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.*;
import simpledb.server.SimpleDB;
import java.io.*;

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
   public static final String agg1 = "select count(sname) from student";
   public static final String agg2 = "select max(gradyear) from student";
   public static final String agg3 = "select min(gradyear) from student";
   public static final String agg4 = "select sum(sid) from student";
   public static final String agg5 = "select avg(sid) from student";
   //public static final String nestedjoin = "select dname, did, cid, title from course, dept where did!=cid order by did, cid desc";
   public static final String nestedjoin = "select sid, sname, gradyear, dname from student, dept where did = majorid and gradyear!=2022 order by gradyear desc, sid";
   public static final String grp1 = "select gradyear from student group by gradyear";
   public static final String grp2 = "select count(sname), gradyear from student group by gradyear";
   public static final String grp3 = "select max(gradyear), majorid from student group by majorid";
   public static final String grp4 = "select min(gradyear), majorid from student group by majorid";
   public static final String grp5 = "select sum(sid), majorid from student group by majorid";
   public static final String grp6 = "select avg(sid), majorid from student group by majorid";
   
   public static final String expt1 = "select sid, sname, gradyear, majorid, eid, sectionid, grade from student, enroll where sid=studentid";
   public static final String expt2 = "Select sname, gradyear, dname, cid, title, sectionid, grade from student, dept, course, enroll, section where did = deptid and majorid=did and sectid = sectionid and cid = courseid and studentid=sid";
   
   public static void main(String[] args) {
      try {
    	 
    	 // Get starting timestamp of function runtime
    	 long start = System.currentTimeMillis();
    	  
         SimpleDB db = new SimpleDB("studentdb");
         Transaction tx  = db.newTx();
         Planner planner = db.planner();
         //QUERY
         String qry = expt2;
         
         Plan p = planner.createQueryPlan(qry, tx);
         Scan s = p.open();
         //HEADER
         //System.out.println("SID\tNAME\tGYEAR\tMAJORID");
         //System.out.println("AGG\tMAJORID");
         
         // expt1
         //System.out.println("SID\tSNAME\tGRADYEAR\tMAJORID\tEID\tSECTIONID\tGRADE");
         
         //expt2
         System.out.println("SNAME\tGRADYEAR\tDNAME\tCID\tTITLE\tSECTIONID\tGRADE");
         
         while (s.next()) {
            //READ VALUES
            //int agg = s.getInt("avgofsid"); //countofsname maxofgradyear minofgradyear sumofsid avgofsid
            //String majorid = s.getString("dname");
            //int majorid = s.getInt("did");
            //String dname = s.getString("dname");
            //int did = s.getInt("did");
            //int cid = s.getInt("cid");
            //String title = s.getString("title");
        	 
        	// expt 1
        	/*
        	int sid = s.getInt("sid");
            String sname = s.getString("sname");
            int gradyear = s.getInt("gradyear");
            int majorid = s.getInt("majorid");
            int eid = s.getInt("eid");
            int sectionid = s.getInt("sectionid");
            String grade = s.getString("grade");
        	 */
        	 
        	//expt 2
        	String sname = s.getString("sname");
            int gradyear = s.getInt("gradyear");
            String dname = s.getString("dname");
            int cid = s.getInt("cid");
            String title = s.getString("title");
            int sectionid = s.getInt("sectionid");
            String grade = s.getString("grade");
        	 

            //PRINT VALUES
            //System.out.println(sid + "\t" + sname + "\t" + gradyear + "\t" + majorid);
            //System.out.println(dname + "\t" + did + "\t" + cid + "\t" + title);
            //System.out.println(agg);
            //expt 1
            //System.out.println(sid + "\t" + sname + "\t" + gradyear + "\t" + majorid + "\t" + eid + "\t" + sectionid + "\t" + grade);
            
            //expt 2
            System.out.println(sname + "\t" + gradyear + "\t" + dname + "\t" + cid + "\t" + title + "\t" + sectionid + "\t" + grade);
         
         }
         s.close();
         tx.commit();
         
         // Get ending timestamp of function runtime
         long end = System.currentTimeMillis();
         
         // Print out total runtime of function
         System.out.println("Total runtime takes " + (end - start) + "ms");
         
      } catch(Exception e) {
          e.printStackTrace();    
      }   
   }   
}
