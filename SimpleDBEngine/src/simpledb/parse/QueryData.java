package simpledb.parse;

import java.util.*;

import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private List<String> orderByAttributes;
   private List<String> orderByDirection;
   
   /**
    * Saves the field, table list, predicate, order by attributes, and order by direction for each order by attribute.
    */
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<String> orderByAttributes, List<String> orderByDirection) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.orderByAttributes = orderByAttributes;
      this.orderByDirection = orderByDirection;
   }
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }
   
   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }
   
   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }
   
   /**
    * Returns the attributes mentioned in the order by clause.
    * @return a list of order by attributes
    */
   public List<String> orderByAttributes() {
      return orderByAttributes;
   }
   
   /**
    * Returns the direction for each attribute mentioned
    * in the order by clause.
    * @return a list of order by direction for each order by attribute
    */
   public List<String> orderByDirection() {
      return orderByDirection;
   }
   
   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      return result;
   }
}
