import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class PartitionedHashJoinDriver {

    private int numPartitions;
    int a = 0;



    public PartitionedHashJoinDriver(int numPartitions){
        this.numPartitions = numPartitions;

    }

    private int hashFunc(int code){
        return code % numPartitions;
    }
    public ArrayList<ArrayList<Integer>> partitionedHashJoin(ArrayList<ArrayList<Integer>> table1, int index1, ArrayList<ArrayList<Integer>> table2,int index2)  {
        ArrayList<ArrayList<ArrayList<Integer>>> partitions1 = new ArrayList<ArrayList<ArrayList<Integer>>>();
        ArrayList<ArrayList<ArrayList<Integer>>> partitions2 = new ArrayList<ArrayList<ArrayList<Integer>>>();
        for(int i = 0; i < numPartitions; i++){
            partitions1.add(i,new ArrayList<ArrayList<Integer>>());
            partitions2.add(i,new ArrayList<ArrayList<Integer>>());
        }
        int n1 = table1.size();
        int n2 = table2.size();
        for(int i = 0; i < Math.max(n1,n2); i++){
            if(i < n1){
                int hash_result = hashFunc(table1.get(i).get(index1));
                partitions1.get(hash_result).add(table1.get(i));
            }
            if(i < n2){
                int hash_result = hashFunc(table2.get(i).get(index2));
                partitions2.get(hash_result).add(table2.get(i));
            }
        }
        ArrayList<ArrayList<Integer>> newTable = new ArrayList<ArrayList<Integer>>();
        ArrayList<HashJoinThread> threads = new ArrayList<HashJoinThread>();

        for(int i = 0; i < numPartitions; i++){
            HashJoinThread hashJoinThread = new HashJoinThread(new HashJoinDriver(),partitions1.get(i),index1,partitions2.get(i),index2);
            hashJoinThread.start();
            threads.add(hashJoinThread);
        }
        for(HashJoinThread thread : threads){
            try {
                thread.join();
                newTable.addAll(thread.getTable());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
        return newTable;
    }





}
