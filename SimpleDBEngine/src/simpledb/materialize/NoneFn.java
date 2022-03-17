package simpledb.materialize;

import simpledb.query.*;

public class NoneFn implements AggregationFn {
   private String fldname;
   private Constant val;
   
   public NoneFn(String fldname) {
      this.fldname = fldname;
   }

   public void processFirst(Scan s) {
      val = s.getVal(fldname);
   }

   public void processNext(Scan s) {
      val = s.getVal(fldname);
   }

   public String fieldName() {
      return fldname;
   }

   public Constant value() {
      return val;
   }

}
