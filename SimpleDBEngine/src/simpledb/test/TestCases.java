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
    public static final String order4 = "select sid, sname, gradyear, majorid from student order by gradyear, majorid";
    public static final String sort5 = "select sid, sname, gradyear, majorid from student order by gradyear, majorid desc";
    public static final String distinct1 = "select distinct sid, sname, gradyear, majorid from student";
    public static final String distinct2 = "select distinct gradyear, majorid from student";
    public static final String agg1 = "select sid, sname, gradyear, avg(majorid) from student";
	 //public static final String nestedjoin = "select dname, did, cid, title from course, dept where did!=cid order by did, cid desc";
	 public static final String nestedjoin = "select sid, sname, gradyear, dname from student, dept where did = majorid and gradyear!=2022 order by gradyear desc, sid";
	 public static final String grp1 = "select sid, sname, gradyear, majorid from student group by sid";
    public static final String grp2 = "select gradyear from student group by gradyear";
	 
	 public static void main(String[] args) {		
	     try {
	         SimpleDB db = new SimpleDB("studentdb");
	         Transaction tx  = db.newTx();
	         Planner planner = db.planner();
	         
	         //QUERY
	         String qry = nestedjoin;
	         
	         Plan p = planner.createQueryPlan(qry, tx);
	         Scan s = p.open();
	         //HEADER
	         //System.out.println("Sid\tName\tYear\tMajorID");
	         
	         //AGGREGATE
	         /*
	         s.next();
	         int agg = s.getInt("avgofmajorid");
	         System.out.println(agg);
	         */
	           
	         while (s.next()) {
	             //READ VALUES
	        	    int sid = s.getInt("sid");
	        	    String sname = s.getString("sname");
	        	    int gradyear = s.getInt("gradyear");
	        	    //int majorid = s.getInt("majorid");
	        	    String majorid = s.getString("dname");
	        	    //int majorid = s.getInt("did");
	        	    /*
	        	    String dname = s.getString("dname");
	        	    int did = s.getInt("did");
	        	    int cid = s.getInt("cid");
	        	    String title = s.getString("title");
	        	    */
	        	    
	        	    //PRINT VALUES
	        	    System.out.println(sid + "\t" + sname + "\t" + gradyear + "\t" + majorid );
	             //System.out.println(dname + "\t" + did + "\t" + cid + "\t" + title );
	         }
	         s.close();
	         tx.commit();
	     } catch(Exception e) {
	         e.printStackTrace();
	     }
    }
}
