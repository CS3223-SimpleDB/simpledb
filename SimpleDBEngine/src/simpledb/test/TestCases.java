package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.*;
import simpledb.server.SimpleDB;
import java.io.*;

public class TestCases {
   //SELECT
   public static final String select1 = "select sid, sname, gradyear, majorid from student";
   //NON-EQUI
   public static final String nonequi1 = "select sid, sname, gradyear, majorid from student where sid < 5";
   public static final String nonequi2 = "select sid, sname, gradyear, majorid from student where sid > 5";
   public static final String nonequi3 = "select sid, sname, gradyear, majorid from student where sid <= 5";
   public static final String nonequi4 = "select sid, sname, gradyear, majorid from student where sid >= 5";
   public static final String nonequi5 = "select sid, sname, gradyear, majorid from student where sid != 5";
   public static final String nonequi6 = "select sid, sname, gradyear, majorid from student where sid <> 5";
   //ORDER BY
   public static final String order1 = "select sid, sname, gradyear, majorid from student order by gradyear";
   public static final String order2 = "select sid, sname, gradyear, majorid from student order by gradyear asc";
   public static final String order3 = "select sid, sname, gradyear, majorid from student order by gradyear desc";
   public static final String order4 = "select sid, sname, gradyear, majorid from student order by gradyear, sid";
   public static final String order5 = "select sid, sname, gradyear, majorid from student order by gradyear, sid desc";
   //DISTINCT
   public static final String distinct1 = "select distinct sid, sname, gradyear, majorid from student";
   public static final String distinct2p1 = "select gradyear, majorid from student order by gradyear";
   public static final String distinct2p2 = "select distinct gradyear, majorid from student";
   //AGGREGATE
   public static final String agg1 = "select count(sname) from student";
   public static final String agg2 = "select max(gradyear) from student";
   public static final String agg3 = "select min(gradyear) from student";
   public static final String agg4 = "select sum(sid) from student";
   public static final String agg5 = "select avg(gradyear) from student";
   public static final String agg6 = "select count(sname), min(gradyear), max(gradyear), sum(sid), avg(sid) from student";
   //GROUP BY
   public static final String grp1 = "select count(sname), gradyear from student group by gradyear";
   public static final String grp2 = "select max(gradyear), majorid from student group by majorid";
   public static final String grp3 = "select min(gradyear), majorid from student group by majorid";
   public static final String grp4 = "select sum(sid), majorid from student group by majorid";
   public static final String grp5 = "select avg(gradyear), majorid from student group by majorid";
   public static final String grp6 = "select count(sid), majorid, gradyear from student group by majorid, gradyear";
   public static final String grp7 = "select count(eid), sectionid, grade from enroll group by sectionid, grade";
   public static final String grp8 = "select count(sid), sname, majorid, gradyear from student group by sname, majorid, gradyear";
   public static final String grp9 = "select count(eid), studentid, sectionid, grade from enroll group by studentid, sectionid, grade";
   public static final String grp10 = "select count(eid), grade from enroll group by grade";
   public static final String grp11 = "select count(sectid), yearoffered from section group by yearoffered";
   public static final String grp12 = "select count(title), deptid from course group by deptid";
   public static final String grp13 = "select count(did), dname from dept group by dname";
   //2-WAY JOIN
   public static final String expt1 = "select sid, sname, gradyear, majorid, eid, sectionid, grade from student, enroll where sid=studentid";
   //4-WAY JOIN
   public static final String expt2 = "select sname, gradyear, dname, cid, title, sectionid, grade from student, dept, course, enroll, section where did = deptid and majorid=did and sectid = sectionid and cid = courseid and studentid=sid";
   //public static final String nestedjoin1 = "select sid, sname, gradyear, dname from student, dept where did = majorid and gradyear!=2022 order by gradyear desc, sid";
   //public static final String nestedjoin2 = "select dname, did, cid, title from course, dept where did!=cid order by did, cid desc";   
   public static void main(String[] args) {
      try {
         // Get starting timestamp of function runtime
         long start = System.currentTimeMillis();
         SimpleDB db = new SimpleDB("studentdb");
         Transaction tx  = db.newTx();
         Planner planner = db.planner();
         
         //QUERY
         String qry = select1;
         
         Plan p = planner.createQueryPlan(qry, tx);
         Scan s = p.open();
         
         //SELECT
         //System.out.println("SID\tNAME\tGYEAR\tMAJORID");
         
         //AGGREGATE
         //agg1
         //System.out.println("COUNT");
         //agg2
         //System.out.println("MAX");
         //agg3
         //System.out.println("MIN");
         //agg4
         //System.out.println("SUM");
         //agg5
         //System.out.println("AVG");
         //agg6
         //System.out.println("COUNT\tMIN\tMAX\tSUM\tAVG");
         
         //GROUP BY
         //grp1
         //System.out.println("COUNT\tGYEAR");
         //grp2
         //System.out.println("MAX\tMAJORID");
         //grp3
         //System.out.println("MIN\tMAJORID");
         //grp4
         //System.out.println("SUM\tMAJORID");
         //grp5
         //System.out.println("AVG\tMAJORID");
         //grp6
         //System.out.println("COUNT\tMAJORID\tGYEAR");
         //grp7
         //System.out.println("COUNT\tSECTID\tGRADE");
         //grp8
         //System.out.println("COUNT\tSNAME\tMAJORID\tGYEAR");
         //grp8
         //System.out.println("COUNT\tSNAME\tMAJORID\tGYEAR");
         //grp9
         //System.out.println("COUNT\tSNAME\tMAJORID\tGYEAR");
         //grp10
         //System.out.println("COUNT\tGRADE");
         //grp11
         //System.out.println("COUNT\tYEAR");
         //grp12
         //System.out.println("COUNT\tDEPTID");
         //grp13
         //System.out.println("COUNT\tDNAME");
         
         //EXPT 1
         //System.out.println("SID\tSNAME\tGRADYEAR\tMAJORID\tEID\tSECTIONID\tGRADE");
         
         //EXPT 2
         //System.out.println("SNAME\tGRADYEAR\tDNAME\tCID\tTITLE\tSECTIONID\tGRADE");
         
         while (s.next()) {
            
            //SELECT
            /*
            int sid = s.getInt("sid");
            String sname = s.getString("sname");
            int gradyear = s.getInt("gradyear");
            int majorid = s.getInt("majorid");
            System.out.println(sid + "\t" + sname + "\t" + gradyear + "\t" + majorid);
            */
            
            //AGGREGATE
            /*
            //agg1
            int countofsname = s.getInt("countofsname");
            System.out.println(countofsname);
            */
            /*
            //agg2
            int maxofgradyear = s.getInt("maxofgradyear");
            System.out.println(maxofgradyear);
            */
            /*
            //agg3
            int minofgradyear = s.getInt("minofgradyear");
            System.out.println(minofgradyear);
            */
            /*
            //agg4
            int sumofsid = s.getInt("sumofsid");
            System.out.println(sumofsid);
            */
            /*
            //agg5
            int avgofgradyear = s.getInt("avgofgradyear");
            System.out.println(avgofgradyear);
            */
            /*
            //agg6
            int countofsname = s.getInt("countofsname");
            int minofgradyear = s.getInt("minofgradyear");
            int maxofgradyear = s.getInt("maxofgradyear");
            int sumofsid = s.getInt("sumofsid");
            int avgofsid = s.getInt("avgofsid");
            System.out.println(countofsname + "\t" + minofgradyear + "\t" + maxofgradyear + "\t" + sumofsid + "\t" + avgofsid);
            */
            
            //GROUP BY
            /*
            //grp1
            int countofsname = s.getInt("countofsname");
            int gradyear = s.getInt("gradyear");
            System.out.println(countofsname + "\t" + gradyear);
            */
            /*
            //grp2
            int maxofgradyear = s.getInt("maxofgradyear");;
            int majorid = s.getInt("majorid");
            System.out.println(maxofgradyear + "\t" + majorid);
            */
            /*
            //grp3
            int minofgradyear = s.getInt("minofgradyear");
            int majorid = s.getInt("majorid");
            System.out.println(minofgradyear + "\t" + majorid);
            */
            /*
            //grp4
            int sumofsid = s.getInt("sumofsid");
            int majorid = s.getInt("majorid");
            System.out.println(sumofsid + "\t" + majorid);
            */
            /*
            //grp5
            int avgofgradyear = s.getInt("avgofgradyear");
            int majorid = s.getInt("majorid");
            System.out.println(avgofgradyear + "\t" + majorid);
            */
            /*
            //grp6
            int countofsid = s.getInt("countofsid");
            int majorid = s.getInt("majorid");
            int gradyear = s.getInt("gradyear");
            System.out.println(countofsid + "\t" + majorid + "\t" + gradyear);
            */
            /*
            //grp7
            int countofeid = s.getInt("countofeid");
            int sectionid = s.getInt("sectionid");
            String grade = s.getString("grade");
            System.out.println(countofeid + "\t" + sectionid + "\t" + grade);
            */
            /*
            //grp8
            int countofsid = s.getInt("countofsid");
            String sname = s.getString("sname");
            int majorid = s.getInt("majorid");
            int gradyear = s.getInt("gradyear");
            System.out.println(countofsid + "\t" + sname + "\t" + majorid + "\t" + gradyear);
            */
            /*
            //grp9
            int countofsid = s.getInt("countofsid");
            String sname = s.getString("sname");
            int majorid = s.getInt("majorid");
            int gradyear = s.getInt("gradyear");
            System.out.println(countofsid + "\t" + sname + "\t" + majorid + "\t" + gradyear);
            */
            /*
            //grp10
            int countofeid = s.getInt("countofeid");
            String grade = s.getString("grade");
            System.out.println(countofeid + "\t" + grade);
            */
            /*
            //grp11
            int countofsectid = s.getInt("countofsectid");
            int yearoffered = s.getInt("yearoffered");
            System.out.println(countofsectid + "\t" + yearoffered);
            */
            /*
            //grp12
            int countoftitle = s.getInt("countoftitle");
            int deptid = s.getInt("deptid");
            System.out.println(countoftitle + "\t" + deptid);
            */
            /*
            //grp13
            int countofdid = s.getInt("countofdid");
            String dname = s.getString("dname");
            System.out.println(countofdid + "\t" + dname);
            */
            
            //2-WAY JOIN
            /*
            int sid = s.getInt("sid");
            String sname = s.getString("sname");
            int gradyear = s.getInt("gradyear");
            int majorid = s.getInt("majorid");
            int eid = s.getInt("eid");
            int sectionid = s.getInt("sectionid");
            String grade = s.getString("grade");
            System.out.println(sid + "\t" + sname + "\t" + gradyear + "\t" + majorid + "\t" + eid + "\t" + sectionid + "\t" + grade);
            */
        	   
        	   //4-WAY JOIN
        	   /*
        	   String sname = s.getString("sname");
            int gradyear = s.getInt("gradyear");
            String dname = s.getString("dname");
            int cid = s.getInt("cid");
            String title = s.getString("title");
            int sectionid = s.getInt("sectionid");
            String grade = s.getString("grade");
            System.out.println(sname + "\t" + gradyear + "\t" + dname + "\t" + cid + "\t" + title + "\t" + sectionid + "\t" + grade);
             */
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
