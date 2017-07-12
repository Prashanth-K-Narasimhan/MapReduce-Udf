package treetop;

import java.util.Comparator;
 
 public class Salary {
 
 private int sum;
 
     public int getSum() {
 
           return sum;
 
     }
 
     public void setSum(int sum) {
 
  this.sum = sum;
 
     }
 
public Salary(int sum) {
 
           super();
 
           this.sum = sum;
 
     }
}
 
class MySalaryComp1 implements Comparator<Salary>{
 
    @Override
 
    public int compare(Salary e1, Salary e2) {
 
        if(e1.getSum()>e2.getSum()){
 
            return 1;
 
        } else {
 
            return -1;
 
        }
 
    }
 
}