package simpledb.test;

import simpledb.tx.Transaction;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;

public class CreateStudentDB {
   public static void main(String[] args) {
      try {
         SimpleDB db = new SimpleDB("studentdb");
         Transaction tx = db.newTx();
         Planner planner = db.planner();
         
         String createStudentTable = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
         planner.executeUpdate(createStudentTable, tx);        
         System.out.println("Table STUDENT created.");
         
         String insertStudentRecords = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
         String[] studvals = {"(1, 'joe', 10, 2021)",
               "(2, 'amy', 20, 2020)",
               "(3, 'max', 10, 2022)",
               "(4, 'sue', 20, 2022)",
               "(5, 'bob', 30, 2020)",
               "(6, 'kim', 20, 2020)",
               "(7, 'art', 30, 2021)",
               "(8, 'pat', 20, 2019)",
               "(9, 'lee', 10, 2021)"};
         for (int i=0; i<studvals.length; i++)
            planner.executeUpdate(insertStudentRecords + studvals[i], tx);
         System.out.println("STUDENT records inserted.");

         String createDeptTable = "create table DEPT(DId int, DName varchar(8))";
         planner.executeUpdate(createDeptTable, tx);
         System.out.println("Table DEPT created.");
         
         String createIndexStudentid = "create index studentid on student(sid)";
         planner.executeUpdate(createIndexStudentid, tx);
         System.out.println("Added studentid index");
         
         String createIndexMajorid = "create index mid on student(majorid)";
         planner.executeUpdate(createIndexMajorid, tx);
         System.out.println("Added majorid index");

         String insertDeptRecords = "insert into DEPT(DId, DName) values ";
         String[] deptvals = {"(10, 'compsci')",
                              "(20, 'math')",
                              "(30, 'drama')"};
         for (int i=0; i<deptvals.length; i++)
            planner.executeUpdate(insertDeptRecords + deptvals[i], tx);
         System.out.println("DEPT records inserted.");

         String createCourseTable = "create table COURSE(CId int, Title varchar(20), DeptId int)";
         planner.executeUpdate(createCourseTable, tx);
         System.out.println("Table COURSE created.");

         String insertCourseTable = "insert into COURSE(CId, Title, DeptId) values ";
         String[] coursevals = {"(12, 'db systems', 10)",
                                "(22, 'compilers', 10)",
                                "(32, 'calculus', 20)",
                                "(42, 'algebra', 20)",
                                "(52, 'acting', 30)",
                                "(62, 'elocution', 30)"};
         for (int i=0; i<coursevals.length; i++)
            planner.executeUpdate(insertCourseTable + coursevals[i], tx);
         System.out.println("COURSE records inserted.");

         String createSectionTable = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
         planner.executeUpdate(createSectionTable, tx);
         System.out.println("Table SECTION created.");

         String insertSectionTable = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
         String[] sectvals = {"(13, 12, 'turing', 2018)",
                              "(23, 12, 'turing', 2019)",
                              "(33, 32, 'newton', 2019)",
                              "(43, 32, 'einstein', 2017)",
                              "(53, 62, 'brando', 2018)"};
         for (int i=0; i<sectvals.length; i++)
            planner.executeUpdate(insertSectionTable + sectvals[i], tx);
         System.out.println("SECTION records inserted.");

         String createEnrollTable = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
         planner.executeUpdate(createEnrollTable, tx);
         System.out.println("Table ENROLL created.");

         String insertEnrollTable = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
         String[] enrollvals = {"(14, 1, 13, 'A')",
                                "(24, 1, 43, 'C' )",
                                "(34, 2, 43, 'B+')",
                                "(44, 4, 33, 'B' )",
                                "(54, 4, 53, 'A' )",
                                "(64, 6, 53, 'A' )"};
         for (int i=0; i<enrollvals.length; i++)
            planner.executeUpdate(insertEnrollTable + enrollvals[i], tx);
         System.out.println("ENROLL records inserted.");
         
         tx.commit();
      } catch(Exception e) {
         e.printStackTrace();
      }
   }
}
