import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
        int a = 0;
        BufferedReader bufferedReader = new BufferedReader(new FileReader("100K.txt"));
        String line = bufferedReader.readLine();
        ArrayList<ArrayList<Integer>> follows = new ArrayList<>();
        ArrayList<ArrayList<Integer>> friendOf = new ArrayList<>();
        ArrayList<ArrayList<Integer>> likes = new ArrayList<>();
        ArrayList<ArrayList<Integer>> hasReview = new ArrayList<>();
        int i = 0;
        while(line != null){
            String[] items = line.split("\\s+");
            String tableName = items[1].split(":")[1];
            if(tableName.equals("follows") || tableName.equals("friendOf") || tableName.equals("likes") || tableName.equals("hasReview")){
                int subject = encode(items[0].split(":")[1]);
                int object = encode(items[2].split(":")[1]);
                ArrayList<Integer> element = new ArrayList<Integer>(Arrays.asList(subject,object));
                if(tableName.equals("follows")) {
                    follows.add(element);
                }
                else if(tableName.equals("friendOf")){
                    friendOf.add(element);
                }else if(tableName.equals("likes")){
                    likes.add(element);
                }else if(tableName.equals("hasReview")){
                    hasReview.add(element);
                }
            }


            line = bufferedReader.readLine();



        }

        HashJoinDriver hashJoinDriver = new HashJoinDriver();

        double N = 100.0;

        double total = 0;
        for(double j = 0; j < N; j++){
            double begin = System.currentTimeMillis();
            ArrayList<ArrayList<Integer>> joinedTable = hashJoinDriver.hashJoin(follows,1,friendOf,0);
            ArrayList<ArrayList<Integer>> joinedTable2 = hashJoinDriver.hashJoin(joinedTable,2,likes,0);
            ArrayList<ArrayList<Integer>> joinedTable3 = hashJoinDriver.hashJoin(joinedTable2,3,hasReview,0);
            double end = System.currentTimeMillis();
            total += (end - begin) / 1000.0;
        }
        double averageTime = total/N;
        System.out.println("Average run time of hash join over 100 runs: " + String.format("%.2f", averageTime));

        PartitionedHashJoinDriver partitionedHashJoinDriver = new PartitionedHashJoinDriver(16);
        double totalPHJ = 0;
        for(double j = 0; j < N; j++){
            double beginPHJ = System.currentTimeMillis();
            ArrayList<ArrayList<Integer>> joinedTablePHJ = partitionedHashJoinDriver.partitionedHashJoin(follows,1,friendOf,0);
            ArrayList<ArrayList<Integer>> joinedTablePHJ2 = partitionedHashJoinDriver.partitionedHashJoin(joinedTablePHJ,2,likes,0);
            ArrayList<ArrayList<Integer>> joinedTablePHJ3 = partitionedHashJoinDriver.partitionedHashJoin(joinedTablePHJ2,3,hasReview,0);
            double endPHJ = System.currentTimeMillis();
            totalPHJ += (endPHJ - beginPHJ) / 1000.0;
        }
        double averageTimePHJ = totalPHJ/N;
        System.out.println("Average run time of improved hash-join algorithm over 100 times: " + String.format("%.2f", averageTimePHJ));



        SortMergeJoinDriver sortMergeJoinDriver = new SortMergeJoinDriver();
        double totalSMJ= 0;
        for(double j = 0; j < N; j++){
            double beginSMJ = System.currentTimeMillis();
            ArrayList<ArrayList<Integer>> joinedTableSMJ = sortMergeJoinDriver.sortMergeJoin(follows,1,friendOf,0);
            ArrayList<ArrayList<Integer>> joinedTableSMJ2 = sortMergeJoinDriver.sortMergeJoin(joinedTableSMJ,2,likes,0);
            ArrayList<ArrayList<Integer>> joinedTableSMJ3 = sortMergeJoinDriver.sortMergeJoin(joinedTableSMJ2,3,hasReview,0);

            double endSMJ = System.currentTimeMillis();
            totalSMJ += (endSMJ - beginSMJ) / 1000.0;
        }
        double averageTimeSMJ = totalSMJ/N;
        System.out.println("Average run time of sort-merge join over 100 runs: " + String.format("%.2f", averageTimeSMJ));

    }
    public static int encode(String code){
        int n = code.length();
        if(code.startsWith("User")){
            int id = Integer.parseInt(code.substring(4,n));
            return (1 << 29) + id;
        }else if(code.startsWith("Product")){
            int id = Integer.parseInt(code.substring(7,n));
            return (2 << 29) + id;
        }else if(code.startsWith("Review")){
            int id = Integer.parseInt(code.substring(6,n));
            return (3 << 29) + id;
        }
        return 0;
    }
    public static String decode(int code){
        int prefix = code / (1 << 29);
        int id = code % (1 << 29);
        if(prefix == 1){
            return "User" + id;
        }else if(prefix == 2){
            return "Product" + id;
        }else if(prefix == 3){
            return "Review" + id;
        }
        return "";
    }
}