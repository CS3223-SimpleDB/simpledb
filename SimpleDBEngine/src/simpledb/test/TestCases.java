package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.*;
import simpledb.server.SimpleDB;

public class TestCases {
	
	// String constants for query testcases
	public static final String nonEqualityQuery1 = "select sid, sname, gradyear, majorid from student where sid < 3";
	public static final String nonEqualityQuery2 = "select sid, sname, gradyear, majorid from student where sid > 3";
	public static final String nonEqualityQuery3 = "select sid, sname, gradyear, majorid from student where sid <= 3";
	public static final String nonEqualityQuery4 = "select sid, sname, gradyear, majorid from student where sid >= 3";
	public static final String nonEqualityQuery5 = "select sid, sname, gradyear, majorid from student where sid != 3";
	public static final String nonEqualityQuery6 = "select sid, sname, gradyear, majorid from student where sname > 'max'";
	public static final String nonEqualityQuery7 = "select sid, sname, gradyear, majorid from student where sname > 'b'";
	
	public static final String sortingQuery1 = "select sid, sname, gradyear, majorid from student order by GradYear asc, Sname desc";
	public static final String sortingQuery2 = "select sid, sname, gradyear, majorid from student order by GradYear desc, Sname asc";
	public static final String sortingQuery3 = "select sid, sname, gradyear, majorid from student order by GradYear asc, MajorId desc";
	public static final String sortingQuery4 = "select sid, sname, gradyear, majorid from student order by MajorId asc, GradYear desc";
	
	public static final String nestedjoin = "select dname, did, cid, title from course, dept where did!=cid order by did, cid desc";
	
	public static void main(String[] args) {		
		try {
	        SimpleDB db = new SimpleDB("studentdb");
	        Transaction tx  = db.newTx();
	        Planner planner = db.planner();
	        
	        // Input testcase String here
	        String qry = nestedjoin;
	        Plan p = planner.createQueryPlan(qry, tx);
	        
	        Scan s = p.open();
	        
	        System.out.println("Sid\tName\tYear\tMajorID");
	        while (s.next()) {
	        	//int sid = s.getInt("sid");
	        	//String sname = s.getString("sname");
	        	//int gradyear = s.getInt("gradyear");
	        	//int majorid = s.getInt("majorid");
	        	//int majorid = s.getInt("did");
	        	
	        	String dname = s.getString("dname");
	        	int did = s.getInt("did");
	        	int cid = s.getInt("cid");
	        	String title = s.getString("title");
	           
	        	System.out.println(dname + "\t" + did + "\t" + cid + "\t" + title );
	        }
	        s.close();
	        tx.commit();
	      }	catch(Exception e) {
	         e.printStackTrace();
	      }
   }
}
