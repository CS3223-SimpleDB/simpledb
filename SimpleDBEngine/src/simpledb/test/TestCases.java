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
   //INDEX SELECT
   public static final String hashindexselect1 = "select sid, sname from student where sid = 98";
   public static final String hashindexselect2 = "select sid, sname from student where sid = 500"; // no such sid
   public static final String hashindexselect3 = "select cid, title, deptid from course where cid=52";
   public static final String hashindexselect4= "select sectid, courseid, prof, yearoffered from section where sectid=143";
   public static final String hashindexselect5 = "select eid, studentid, sectionid, grade from enroll where grade='A+'";
   public static final String btreeindexselect1 = "select courseid, prof, yearoffered from section where courseid=242";
   public static final String btreeindexselect2 = "select courseid, prof, yearoffered from section where courseid=20"; // no such courseid
   public static final String btreeindexselect3 = "select cid, title, deptid from course where deptid=290";
   public static final String btreeindexselectsort1 = "select courseid, prof, yearoffered from section where courseid=242 order by prof asc";
   //EQUI-PREDICATES
   public static final String equi1 = "select studentid, sectionid, grade from enroll where studentid=99";
   public static final String equi2 = "select studentid, sectionid, grade from enroll where grade='B+' and studentid=99";
   public static final String equi3 = "select studentid, sectionid, grade from enroll where grade='A' and studentid=99"; // no results
   public static final String equi4 = "select cid, title, deptid from course where deptid=410 order by title asc";
   public static final String equi5 = "select sectid, courseid, prof, yearoffered from section where yearoffered=1966 order by prof asc"; // no results
   public static final String equi6 = "select sid, sname, gradyear, majorid from student where gradyear=1994 order by majorid";
   public static final String equi7 = "select sid, sname, gradyear, majorid from student where gradyear=1994 and majorid=180";
   //NON-EQUI
   public static final String nonequi1 = "select sid, sname, gradyear, majorid from student where sid < 5";
   public static final String nonequi2 = "select sid, sname, gradyear, majorid from student where sid > 5";
   public static final String nonequi3 = "select sid, sname, gradyear, majorid from student where sid <= 5";
   public static final String nonequi4 = "select sid, sname, gradyear, majorid from student where sid >= 5";
   public static final String nonequi5 = "select sid, sname, gradyear, majorid from student where sid != 5";
   public static final String nonequi6 = "select sid, sname, gradyear, majorid from student where sid <> 5";
   public static final String nonequi7 = "select cid, title, deptid from course where cid > 200";
   public static final String nonequi8 = "select sectid, courseid, prof, yearoffered from section where sectid < 300";
   //ORDER BY
   public static final String order1 = "select sid, sname, gradyear, majorid from student order by gradyear";
   public static final String order2 = "select sid, sname, gradyear, majorid from student order by gradyear asc";
   public static final String order3 = "select sid, sname, gradyear, majorid from student order by gradyear desc";
   public static final String order4 = "select sid, sname, gradyear, majorid from student order by gradyear, sid";
   public static final String order5 = "select sid, sname, gradyear, majorid from student order by gradyear, sid desc";
   public static final String order6 = "select cid, title, deptid from course order by title desc";
   //DISTINCT
   public static final String distinct1 = "select distinct sid, sname, gradyear, majorid from student";
   public static final String distinct2p1 = "select gradyear, majorid from student order by gradyear";
   public static final String distinct2p2 = "select distinct gradyear, majorid from student order by gradyear";
   public static final String distinct3 = "select distinct grade from enroll";
   //AGGREGATE
   public static final String agg1 = "select count(sname) from student";
   public static final String agg2 = "select min(gradyear) from student";
   public static final String agg3 = "select max(gradyear) from student";
   public static final String agg4 = "select avg(gradyear) from student";
   public static final String agg5 = "select sum(sid) from student";
   public static final String agg6 = "select count(sname), min(gradyear), max(gradyear), avg(gradyear), sum(sid) from student"; //multiple aggregates
   //GROUP BY
   public static final String grp1 = "select gradyear, count(sname) from student group by gradyear";        //student
   public static final String grp2 = "select majorid, max(gradyear) from student group by majorid";         //student
   public static final String grp3 = "select majorid, min(gradyear) from student group by majorid";         //student
   public static final String grp4 = "select majorid, sum(sid) from student group by majorid";              //student
   public static final String grp5 = "select majorid, avg(gradyear) from student group by majorid";         //student
   public static final String grp6 = "select grade, count(eid) from enroll group by grade";                 //enroll
   public static final String grp7 = "select yearoffered, count(sectid) from section group by yearoffered"; //section
   public static final String grp8 = "select deptid, count(title) from course group by deptid";             //course
   public static final String grp9 = "select dname, count(did) from dept group by dname";                   //dept
   public static final String grp10 = "select majorid, gradyear, count(sid) from student group by majorid, gradyear"; //2 group by fields
   public static final String grp11 = "select sectionid, grade, count(eid) from enroll group by sectionid, grade"; //2 group by fields
   public static final String grp12 = "select sname, majorid, gradyear, count(sid) from student group by sname, majorid, gradyear"; //3 group by fields
   public static final String grp13 = "select studentid, sectionid, grade, count(eid) from enroll group by studentid, sectionid, grade"; //3 group by fields
   //2-WAY JOIN
   public static final String expt1 = "select sid, sname, gradyear, majorid, eid, sectionid, grade from student, enroll where sid=studentid";
   //4-WAY JOIN
   public static final String expt2 = "select sname, gradyear, dname, cid, title, sectionid, grade from student, dept, course, enroll, section where did = deptid and majorid=did and sectid = sectionid and cid = courseid and studentid=sid";
   
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
         //HEADER
         //System.out.println("SID\tNAME\tGYEAR\tMAJORID");
         //expt 1
         //System.out.println("SID\tSNAME\tGRADYEAR\tMAJORID\tEID\tSECTIONID\tGRADE");
         //expt 2
         //System.out.println("SNAME\tGRADYEAR\tDNAME\tCID\tTITLE\tSECTIONID\tGRADE");
         while (s.next()) {
            //READ & PRINT VALUES
            /*
            int sid = s.getInt("sid");
            String sname = s.getString("sname");
            int gradyear = s.getInt("gradyear");
            int majorid = s.getInt("majorid");
            System.out.println(sid + "\t" + sname + "\t" + gradyear + "\t" + majorid);
            */
            /*
            //expt 1
            int sid = s.getInt("sid");
            String sname = s.getString("sname");
            int gradyear = s.getInt("gradyear");
            int majorid = s.getInt("majorid");
            int eid = s.getInt("eid");
            int sectionid = s.getInt("sectionid");
            String grade = s.getString("grade");
            System.out.println(sid + "\t" + sname + "\t" + gradyear + "\t" + majorid + "\t" + eid + "\t" + sectionid + "\t" + grade);
            */
        	   /*
        	   //expt 2
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
