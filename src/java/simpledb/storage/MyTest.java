package simpledb.storage;

import java.util.ArrayList;
import java.util.Iterator;

public class MyTest {
    public static void main(String[] args) {
        ArrayList<Integer> li = new ArrayList<>();
        li.add(1);
        li.add(2);

        Iterator<Integer> iterator = li.iterator();

        while (iterator.hasNext()){
            System.out.println(iterator.next());
            System.out.println(iterator);
            System.out.println(iterator.hasNext());
        }


    }
}
