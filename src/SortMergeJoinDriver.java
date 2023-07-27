import java.util.ArrayList;
import java.util.Comparator;

public class SortMergeJoinDriver {
    public ArrayList<ArrayList<Integer>> sortMergeJoin(ArrayList<ArrayList<Integer>> table1, int index1, ArrayList<ArrayList<Integer>> table2, int index2){
       table1.sort(Comparator.comparing(x->x.get(index1)));
       table2.sort(Comparator.comparing(x->x.get(index2)));
       ArrayList<ArrayList<Integer>> newTable = new ArrayList<ArrayList<Integer>>();
       int n1 = table1.size();
       int n2 = table2.size();
       int i = 0;
       int j = 0;
       while (i < n1 && j < n2){
           if(table1.get(i).get(index1).compareTo(table2.get(j).get(index2)) == 0) {
               int oldJ = j;
               while (j < n2 && table1.get(i).get(index1).compareTo(table2.get(j).get(index2)) == 0) {
                   ArrayList<Integer> element = new ArrayList<Integer>();
                   element.addAll(table1.get(i));
                   element.addAll(table2.get(j));
                   element.remove(index1);
                   newTable.add(element);
                   j++;
               }
               j = oldJ;
               i++;
           }
           else if(table1.get(i).get(index1).compareTo(table2.get(j).get(index2)) < 0){
                   i++;
           }else{
                    j++;
           }
        }
       return newTable;
    }
}
