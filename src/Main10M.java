
import java.io.BufferedReader;
        import java.io.FileReader;
        import java.io.IOException;
        import java.util.ArrayList;
        import java.util.Arrays;

public class Main10M {
    public static void main(String[] args) throws IOException {

        ArrayList<ArrayList<Integer>> follows = new ArrayList<ArrayList<Integer>>();
        ArrayList<ArrayList<Integer>> friendOf = new ArrayList<ArrayList<Integer>>();
        ArrayList<ArrayList<Integer>> likes = new ArrayList<ArrayList<Integer>>();
        ArrayList<ArrayList<Integer>> hasReview = new ArrayList<ArrayList<Integer>>();
        BufferedReader bufferedReader = new BufferedReader(new FileReader("watdiv.10M.nt"));
        ArrayList<String> lines = new ArrayList<String>();
        String line = bufferedReader.readLine();
        while(line != null){
            lines.add(line);
            if(lines.size() == 100000){
                readBatch(lines,follows,friendOf,likes,hasReview);
                lines.clear();
            }
            line = bufferedReader.readLine();

        }
        HashJoinDriver hashJoinDriver = new HashJoinDriver();
        double N = 10.0;
        double total = 0;
        for(double j = 0; j < N; j++){
            double begin = System.currentTimeMillis();
            ArrayList<ArrayList<Integer>> joinedTable = hashJoinDriver.hashJoin(follows,1,friendOf,0);
            ArrayList<ArrayList<Integer>> joinedTable2 = hashJoinDriver.hashJoin(joinedTable,2,likes,0);
            ArrayList<ArrayList<Integer>> joinedTable3 = hashJoinDriver.hashJoin(joinedTable2,3,hasReview,0);
            double end = System.currentTimeMillis();
            total += (end - begin) / 1000.0;
            System.out.println("Join-"  + j + ": " + total);
        }
        double averageTime = total/N;
        System.out.println(averageTime);

    }
    public static void readBatch(ArrayList<String> batchs, ArrayList<ArrayList<Integer>> follows,ArrayList<ArrayList<Integer>> friendOf, ArrayList<ArrayList<Integer>> likes, ArrayList<ArrayList<Integer>> hasReview) throws IOException {
        for(String line : batchs){
            String[] items = line.split("\\s+");
            String[] tableNameList = items[1].replace("<","").replace(">","").split("/");
            String tableName = tableNameList[tableNameList.length - 1];
            if(!(tableName.equals("follows") || tableName.equals("friendOf") || tableName.equals("likes") || tableName.equals("rev#hasReview"))){
                continue;
            }
            String[] subjectNameList = items[0].replace("<","").replace(">","").split("/");
            Integer subject = DataEncoder.encode(subjectNameList[subjectNameList.length-1]);
            String[] objectNameList = items[2].replace("<","").replace(">","").split("/");
            Integer object = DataEncoder.encode(objectNameList[objectNameList.length - 1]);
            ArrayList<Integer> element = new ArrayList<Integer>(Arrays.asList(subject,object));
            if(tableName.equals("follows")){
                follows.add(element);
            }else if(tableName.equals("friendOf")){
                friendOf.add(element);
            }else if(tableName.equals("likes")){
                likes.add(element);
            }else if(tableName.equals("rev#hasReview")){
                hasReview.add(element);
            }
        }

    }

}

