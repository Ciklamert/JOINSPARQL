import java.util.ArrayList;
import java.util.HashMap;

public class HashJoinDriver {

    public ArrayList<ArrayList<Integer>> hashJoin(ArrayList<ArrayList<Integer>> table1, int index1, ArrayList<ArrayList<Integer>> table2,int index2){
        ArrayList<ArrayList<Integer>> newTable = new ArrayList<ArrayList<Integer>>();
        HashMap<Integer,ArrayList<ArrayList<Integer>>> hashTable = new HashMap<Integer,ArrayList<ArrayList<Integer>>>();
        int n1 = table1.size();
        int n2 = table2.size();
        if(n1 < n2){
            for (ArrayList<Integer> element1 : table1){
                if(!hashTable.containsKey(element1.get(index1))){
                    hashTable.put(element1.get(index1),new ArrayList<ArrayList<Integer>>());
                }
                hashTable.get(element1.get(index1)).add(element1);
            }
            for(ArrayList<Integer> element2: table2){
                if(hashTable.containsKey(element2.get(index2))){
                    for(ArrayList<Integer> element1 : hashTable.get(element2.get(index2))) {
                        ArrayList<Integer> element = new ArrayList<Integer>();
                        element.addAll(element1);
                        element.addAll(element2);
                        element.remove(index1);
                        newTable.add(element);
                    }
                }
            }
        }else{
            for (ArrayList<Integer> element2 : table2){
                if(!hashTable.containsKey(element2.get(index2))){
                    hashTable.put(element2.get(index2),new ArrayList<ArrayList<Integer>>());
                }
                hashTable.get(element2.get(index2)).add(element2);
            }
            for(ArrayList<Integer> element1: table1){
                if(hashTable.containsKey(element1.get(index1))){
                    for(ArrayList<Integer> element2 : hashTable.get(element1.get(index1))) {
                        ArrayList<Integer> element = new ArrayList<Integer>();
                        element.addAll(element1);
                        element.addAll(element2);
                        element.remove(index1);
                        newTable.add(element);
                    }
                }
            }
        }
        return newTable;
    }
}
