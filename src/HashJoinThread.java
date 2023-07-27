import java.util.ArrayList;

public class HashJoinThread  extends Thread{
    int a = 0;
    private HashJoinDriver hashJoinDriver;
    private ArrayList<ArrayList<Integer>> table1 = new ArrayList<ArrayList<Integer>>();
    private ArrayList<ArrayList<Integer>> table2 = new ArrayList<ArrayList<Integer>>();
    private int index1;
    private int index2;
    private ArrayList<ArrayList<Integer>> newTable;
    public HashJoinThread(HashJoinDriver hashJoinDriver,ArrayList<ArrayList<Integer>> table1, int index1, ArrayList<ArrayList<Integer>> table2, int index2){
        this.hashJoinDriver = hashJoinDriver;
        this.index1 = index1;
        this.index2 = index2;
        this.table1 = table1;
        this.table2 = table2;
    }
    public void run(){
        newTable = hashJoinDriver.hashJoin(table1,index1,table2,index2);
    }
    public ArrayList<ArrayList<Integer>> getTable(){
        return newTable;
    }

}
