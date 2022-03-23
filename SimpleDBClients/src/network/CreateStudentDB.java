package network;
import java.sql.*;

import simpledb.jdbc.network.NetworkDriver;

public class CreateStudentDB {
   public static void main(String[] args) {
      Driver d = new NetworkDriver();
      String url = "jdbc:simpledb://localhost";

      try (Connection conn = d.connect(url, null);
            Statement stmt = conn.createStatement()) {
         String s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
         stmt.executeUpdate(s);
         System.out.println("Table STUDENT created.");
         
         s = "create index studentid on STUDENT(sid) using hash";
         stmt.executeUpdate(s);
         System.out.println("Added studentid index");
         
         s = "create index studentmajor on STUDENT(majorid) using btree";
         stmt.executeUpdate(s);
         System.out.println("Added studentmajor index");

         s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
         String[] studvals = {"(1, 'joe', 10, 2021)",
                 "(2, 'amy', 20, 2020)",
                 "(3, 'max', 10, 2022)",
                 "(4, 'sue', 20, 2022)",
                 "(5, 'bob', 30, 2020)",
                 "(6, 'kim', 20, 2020)",
                 "(7, 'art', 30, 2021)",
                 "(8, 'pat', 20, 2019)",
                 "(9, 'lee', 10, 2021)",
                 "(10,'melissa',410,2004)",
                 "(11,'maybelle',420,2010)",
                 "(12,'shelba',220,1992)",
                 "(13,'betta',310,2010)",
                 "(14,'cesare',180,1994)",
                 "(15,'vanna',410,1993)",
                 "(16,'anallise',60,2006)",
                 "(17,'palm',120,2009)",
                 "(18,'arv',410,2002)",
                 "(19,'lizzy',290,1991)",
                 "(20,'aveline',80,1998)",
                 "(21,'viviene',280,2004)",
                 "(22,'merle',260,1990)",
                 "(23,'reade',130,2006)",
                 "(24,'janeczka',420,2008)",
                 "(25,'juana',480,1997)",
                 "(26,'ruprecht',380,1993)",
                 "(27,'annadiane',500,2010)",
                 "(28,'jaclyn',260,1964)",
                 "(29,'morey',300,1997)",
                 "(30,'angeline',410,1998)",
                 "(31,'robbin',210,2003)",
                 "(32,'cathy',60,2006)",
                 "(33,'vickie',500,1993)",
                 "(34,'tansy',340,2001)",
                 "(35,'xaviera',410,1997)",
                 "(36,'lannie',100,2010)",
                 "(37,'nessy',490,2001)",
                 "(38,'goober',290,2000)",
                 "(39,'rosana',350,2006)",
                 "(40,'elvina',350,1997)",
                 "(41,'anallise',80,1999)",
                 "(42,'saba',140,1993)",
                 "(43,'dan',150,2008)",
                 "(44,'alric',430,2005)",
                 "(45,'gui',310,1998)",
                 "(46,'waite',310,2002)",
                 "(47,'jerrylee',370,2001)",
                 "(48,'astrix',280,2000)",
                 "(49,'wallace',170,1996)",
                 "(50,'egbert',210,1993)",
                 "(51,'stace',420,1998)",
                 "(52,'nichole',210,2005)",
                 "(53,'annabelle',40,1995)",
                 "(54,'stephine',100,2007)",
                 "(55,'jackie',320,2007)",
                 "(56,'alfy',170,2008)",
                 "(57,'caria',380,2007)",
                 "(58,'heida',50,2001)",
                 "(59,'mathian',430,1993)",
                 "(60,'paulie',120,2011)",
                 "(61,'haskell',430,2009)",
                 "(62,'katinka',130,2007)",
                 "(63,'meade',250,2006)",
                 "(64,'bud',220,1994)",
                 "(65,'niko',330,2001)",
                 "(66,'babita',150,1999)",
                 "(67,'justinn',30,2005)",
                 "(68,'ruthy',130,2000)",
                 "(69,'joly',310,1994)",
                 "(70,'rosco',420,1985)",
                 "(71,'izzy',40,2012)",
                 "(72,'dynah',400,2000)",
                 "(73,'gayelord',150,1994)",
                 "(74,'drusilla',360,1994)",
                 "(75,'micaela',80,2010)",
                 "(76,'murial',450,2000)",
                 "(77,'anthea',340,2004)",
                 "(78,'aurelia',360,2004)",
                 "(79,'rowney',340,2002)",
                 "(80,'tani',270,2009)",
                 "(81,'lana',150,1987)",
                 "(82,'thaddeus',190,1994)",
                 "(83,'jaimie',30,1995)",
                 "(84,'pieter',300,2007)",
                 "(85,'pat',240,1990)",
                 "(86,'clarita',60,1996)",
                 "(87,'denney',320,2001)",
                 "(88,'ashli',100,2011)",
                 "(89,'pavla',380,1993)",
                 "(90,'benoite',50,2009)",
                 "(91,'stephani',120,1996)",
                 "(92,'bernhard',380,1990)",
                 "(93,'aurelia',340,1990)",
                 "(94,'lita',340,1999)",
                 "(95,'sheffie',110,2012)",
                 "(96,'row',190,2001)",
                 "(97,'staford',150,2013)",
                 "(98,'traver',240,2002)",
                 "(99,'alfred',150,1990)",
                 "(100,'tedi',180,1994)"};
         for (int i=0; i<studvals.length; i++)
            stmt.executeUpdate(s + studvals[i]);
         System.out.println("STUDENT records inserted.");

         s = "create table DEPT(DId int, DName varchar(20))";
         stmt.executeUpdate(s);
         System.out.println("Table DEPT created.");
         
         s = "create index deptdid on DEPT(did) using hash";
         stmt.executeUpdate(s);
         System.out.println("Added deptdid index");

         s = "insert into DEPT(DId, DName) values ";
         String[] deptvals = {"(10, 'compsci')",
        		 "(20, 'math')",
		         "(30, 'drama')",
		         "(40,'training')",
		         "(50,'research')",
		         "(60,'legal')",
		         "(70,'development')",
		         "(80,'product management')",
		         "(90,'sales')",
		         "(100,'engineering')",
		         "(110,'management')",
		         "(120,'business')",
		         "(130,'architecture')",
		         "(140,'urban planning')",
		         "(150,'landscaping')",
		         "(160,'asian studies')",
		         "(170,'library science')",
		         "(180,'liberal arts')",
		         "(190,'women studies')",
		         "(200,'art')",
		         "(210,'human resources')",
		         "(220,'music')",
		         "(230,'photography')",
		         "(240,'theatre')",
		         "(250,'drama')",
		         "(260,'cinematography')",
		         "(270,'hotel management')",
		         "(280,'construction')",
		         "(290,'marketing')",
		         "(300,'actuarial science')",
		         "(310,'banking')",
		         "(320,'insurance')",
		         "(330,'communications')",
		         "(340,'dental')",
		         "(350,'medicine')",
		         "(360,'nursing')",
		         "(370,'philosophy')",
		         "(380,'ministry')",
		         "(390,'automotive')",
		         "(400,'aviation')",
		         "(410,'biology')",
		         "(420,'chemistry')",
		         "(430,'physics')",
		         "(440,'zoology')",
		         "(450,'ecology')",
		         "(460,'paralegal')",
		         "(470,'geography')",
		         "(480,'history')",
		         "(490,'language')",
		         "(500,'economics')"
         };
         for (int i=0; i<deptvals.length; i++)
            stmt.executeUpdate(s + deptvals[i]);
         System.out.println("DEPT records inserted.");

         s = "create table COURSE(CId int, Title varchar(20), DeptId int)";
         stmt.executeUpdate(s);
         System.out.println("Table COURSE created.");
         
         s = "create index courseid on COURSE(cid) using hash";
         stmt.executeUpdate(s);
         System.out.println("Added courseid index");
         
         s = "create index coursedeptid on COURSE(deptid) using btree";
         stmt.executeUpdate(s);
         System.out.println("Added coursedeptid index");

         s = "insert into COURSE(CId, Title, DeptId) values ";
         String[] coursevals = {"(12, 'db systems', 10)",
        		 "(22, 'compilers', 10)",
	               "(32, 'calculus', 20)",
	               "(42, 'algebra', 20)",
	               "(52, 'acting', 30)",
		         "(62, 'elocution', 30)",
		         "(72,'waiter',270)",
		         "(82,'financial marketing',290)",
		         "(92,'stemcell research',50)",
		         "(102,'aggressive marketing',290)",
		         "(112,'feminism',190)",
		         "(122,'education training',40)",
		         "(132,'caligraphy',200)",
		         "(142,'theatre presence',240)",
		         "(152,'cameraworks',260)",
		         "(162,'telecommunication',330)",
		         "(172,'team sales',90)",
		         "(182,'eastern music',220)",
		         "(192,'automotive theory',390)",
		         "(202,'dental basics',340)",
		         "(212,'social hierarchies',490)",
		         "(222,'construction',280)",
		         "(232,'contemporary art',200)",
		         "(242,'management handling',210)",
		         "(252,'camera basics',230)",
		         "(262,'prop design',240)",
		         "(272,'leadership',120)",
		         "(282,'pharmacy',350)",
		         "(292,'directing',260)",
		         "(302,'stage presence',240)",
		         "(312,'motorcycles',390)",
		         "(322,'interior design',140)",
		         "(332,'cellular biology',410)",
		         "(342,'biochemistry',410)",
		         "(352,'genetics',410)",
		         "(362,'marine biology',410)",
		         "(372,'aquatic life',410)",
		         "(382,'microbiology',410)",
		         "(392,'immunology',410)",
		         "(402,'aircraft piloting',280)",
		         "(412,'aircraft mechanics',280)",
		         "(422,'autobody repair',280)",
		         "(432,'avionics technology',280)",
		         "(442,'diesel mechanics',280)",
		         "(452,'precision production',280)",
		         "(462,'religion',370)",
		         "(472,'theology',370)",
		         "(482,'bible studies',370)",
		         "(492,'divinity',370)",
		         "(502,'religious education',370)",
		         "(512,'english',490)",
		         "(522,'american literature',490)",
		         "(532,'public speaking',490)",
		         "(542,'creative writing',490)",
		         "(552,'asian languages',490)",
		         "(562,'classical literature',490)",
		         "(572,'linguistics',490)",
		         "(582,'french literature',490)",
		         "(592,'german literature',490)",
		         "(602,'spanish literature',490)",
		         "(612,'architectural drafting',100)",
		         "(622,'civil engineering',100)",
		         "(632,'electrical engineering',100)",
		         "(642,'chemical engineering',100)",
		         "(652,'mechanical engineering',100)",
		         "(662,'industrial production',100)",
		         "(672,'aerospace engineering',100)",
		         "(682,'civil engineering',100)",
		         "(692,'electronics engineering',100)",
		         "(702,'fine arts',200)"};
         for (int i=0; i<coursevals.length; i++)
            stmt.executeUpdate(s + coursevals[i]);
         System.out.println("COURSE records inserted.");

         s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
         stmt.executeUpdate(s);
         System.out.println("Table SECTION created.");
         
         s = "create index sectionid on SECTION(sectid) using hash";
         stmt.executeUpdate(s);
         System.out.println("Added sectionid index");
         
         s = "create index sectioncourseid on SECTION(courseid) using btree";
         stmt.executeUpdate(s);
         System.out.println("Added sectioncourseid index");

         s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
         String[] sectvals = {"(13, 12, 'turing', 2018)",
                 "(23, 12, 'turing', 2019)",
                 "(33, 32, 'newton', 2019)",
                 "(43, 32, 'einstein', 2017)",
                 "(53, 62, 'brando', 2018)",
    	         "(63,422,'frowde',1995)",
    	         "(73,592,'beadham',2009)",
    	         "(83,62,'savage',2007)",
    	         "(93,362,'exell',2020)",
    	         "(103,702,'fireman',2014)",
    	         "(113,322,'meiklem',2011)",
    	         "(123,302,'ludlem',2004)",
    	         "(133,632,'commusso',2013)",
    	         "(143,102,'paull',1991)",
    	         "(153,682,'tewelson',2015)",
    	         "(163,142,'seel',1998)",
    	         "(173,42,'rheam',2007)",
    	         "(183,472,'olivet',2003)",
    	         "(193,602,'lauderdale',1992)",
    	         "(203,22,'mandrier',2016)",
    	         "(213,232,'baack',1990)",
    	         "(223,392,'chate',2015)",
    	         "(233,242,'hatliffe',1996)",
    	         "(243,122,'gringley',2001)",
    	         "(253,42,'inker',2012)",
    	         "(263,52,'tatersale',2007)",
    	         "(273,522,'spellessy',2017)",
    	         "(283,462,'laroux',2002)",
    	         "(293,92,'sollowaye',1992)",
    	         "(303,462,'alexsandrovich',2017)",
    	         "(313,662,'culley',2018)",
    	         "(323,382,'petracchi',2005)",
    	         "(333,182,'lawes',2018)",
    	         "(343,382,'middlehurst',1994)",
    	         "(353,702,'toulmin',2009)",
    	         "(363,42,'espadas',2002)",
    	         "(373,362,'iacobini',1995)",
    	         "(383,582,'hoolaghan',2011)",
    	         "(393,242,'coyish',1992)",
    	         "(403,542,'billinge',1993)",
    	         "(413,242,'walewski',2013)",
    	         "(423,132,'longbone',2017)",
    	         "(433,432,'heskey',2020)",
    	         "(443,62,'spinage',1990)",
    	         "(453,172,'ousby',1990)",
    	         "(463,192,'allon',1997)",
    	         "(473,272,'vlasyev',2004)",
    	         "(483,422,'rushe',2000)",
    	         "(493,492,'wigelsworth',2015)",
    	         "(503,162,'muehle',1994)",
    	         "(513,292,'duthie',2009)",
    	         "(523,622,'brooking',1994)",
    	         "(533,62,'dovidaitis',2007)",
    	         "(543,652,'leavens',2011)",
    	         "(553,492,'pooly',2013)",
    	         "(563,612,'cyphus',2018)",
    	         "(573,192,'goane',2000)",
    	         "(583,312,'rheubottom',2002)",
    	         "(593,472,'egdal',2013)",
    	         "(603,462,'merriday',2016)",
    	         "(613,452,'smetoun',2019)",
    	         "(623,572,'baggalley',1998)",
    	         "(633,382,'pretsel',2002)",
    	         "(643,252,'fitch',1994)",
    	         "(653,362,'melling',2002)",
    	         "(663,492,'rubinowitz',2017)",
    	         "(673,592,'facchini',2011)",
    	         "(683,102,'constantine',2013)",
    	         "(693,142,'criag',2012)",
    	         "(703,462,'muriel',2010)",
    	         "(713,332,'bett',2019)",
    	         "(723,402,'lanney',1998)",
    	         "(733,632,'bispo',1994)",
    	         "(743,452,'pentycross',1998)",
    	         "(753,152,'pinchin',2001)",
    	         "(763,62,'bennen',2018)",
    	         "(773,552,'yearron',2016)",
    	         "(783,482,'cairney',2009)",
    	         "(793,322,'blaise',2014)",
    	         "(803,382,'dearden',1998)",
    	         "(813,652,'bearfoot',2003)",
    	         "(823,282,'mandel',2011)",
    	         "(833,542,'oats',2003)",
    	         "(843,372,'dorrington',1991)",
    	         "(853,372,'farey',1990)",
    	         "(863,122,'cayette',2012)",
    	         "(873,362,'beevors',1992)",
    	         "(883,402,'deppe',2011)",
    	         "(893,92,'claybourn',2004)",
    	         "(903,522,'watson-brown',1998)"};
         for (int i=0; i<sectvals.length; i++)
            stmt.executeUpdate(s + sectvals[i]);
         System.out.println("SECTION records inserted.");

         s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
         stmt.executeUpdate(s);
         System.out.println("Table ENROLL created.");
         
         s = "create index enrollstudent on ENROLL(StudentId) using hash";
         stmt.executeUpdate(s);
         System.out.println("Added enrollstudent index");
         
         s = "create index enrollsection on ENROLL(sectionid) using btree";
         stmt.executeUpdate(s);
         System.out.println("Added enrollsection index");

         s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
         String[] enrollvals = {"(14, 1, 13, 'A')",
                 "(24, 1, 43, 'C' )",
                 "(34, 2, 43, 'B+')",
                 "(44, 4, 33, 'B' )",
                 "(54, 4, 53, 'A' )",
                 "(64, 6, 53, 'A' )",
          		"(74,71,253,'C')",
          		"(84,96,313,'A-')",
          		"(94,28,423,'A+')",
          		"(104,6,773,'A+')",
          		"(114,99,663,'C')",
          		"(124,19,533,'F')",
          		"(134,13,163,'B-')",
          		"(144,68,893,'C+')",
          		"(154,78,753,'C+')",
          		"(164,60,683,'E')",
          		"(174,73,183,'A+')",
          		"(184,25,193,'D')",
          		"(194,57,543,'B+')",
          		"(204,13,763,'D')",
          		"(214,92,93,'D')",
          		"(224,41,683,'A')",
          		"(234,32,333,'B+')",
          		"(244,13,223,'B')",
          		"(254,19,293,'D')",
          		"(264,8,293,'C-')",
          		"(274,70,593,'B+')",
          		"(284,29,113,'A')",
          		"(294,43,903,'A-')",
          		"(304,89,893,'A')",
          		"(314,62,593,'C-')",
          		"(324,93,153,'E')",
          		"(334,90,293,'C+')",
          		"(344,97,853,'E')",
          		"(354,89,103,'B+')",
          		"(364,51,393,'F')",
          		"(374,29,173,'C-')",
          		"(384,72,513,'F')",
          		"(394,31,103,'C+')",
          		"(404,54,723,'D')",
          		"(414,6,83,'E')",
          		"(424,34,233,'B+')",
          		"(434,1,273,'A+')",
          		"(444,8,413,'E')",
          		"(454,87,293,'C-')",
          		"(464,38,733,'F')",
          		"(474,99,323,'B+')",
          		"(484,99,123,'C-')",
          		"(494,44,773,'F')",
          		"(504,27,263,'C')",
          		"(514,54,333,'C')",
          		"(524,16,893,'A')",
          		"(534,43,843,'D')",
          		"(544,82,483,'A+')",
          		"(554,6,773,'B')",
          		"(564,78,443,'A-')",
          		"(574,76,903,'C-')",
          		"(584,72,493,'D')",
          		"(594,25,663,'C-')",
          		"(604,88,743,'F')",
          		"(614,22,893,'C-')",
          		"(624,1,643,'B-')",
          		"(634,73,833,'F')",
          		"(644,67,83,'F')",
          		"(654,33,93,'F')",
          		"(664,34,493,'A+')",
          		"(674,80,373,'C')",
          		"(684,19,563,'E')",
          		"(694,99,303,'D')",
          		"(704,21,413,'A+')",
          		"(714,22,513,'B+')",
          		"(724,36,623,'B+')",
          		"(734,2,863,'A')",
          		"(744,57,813,'F')",
          		"(754,45,143,'F')",
          		"(764,100,863,'A-')",
          		"(774,53,493,'A-')",
          		"(784,42,453,'F')",
          		"(794,82,403,'C-')",
          		"(804,21,393,'A-')"};
         for (int i=0; i<enrollvals.length; i++)
            stmt.executeUpdate(s + enrollvals[i]);
         System.out.println("ENROLL records inserted.");

      }
      catch(SQLException e) {
         e.printStackTrace();
      }
   }
}
