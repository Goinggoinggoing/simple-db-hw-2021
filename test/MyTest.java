import java.io.Serializable;
import java.util.*;

import java.util.HashMap;

public class MyTest {
    public static void main(String[] args) {
        // 创建一个HashMap并向其中添加一个对象
        HashMap<String, MyClass> map = new HashMap<>();
        MyClass obj = new MyClass(new int[]{1, 2, 3});
        map.put("key", obj);

        // 从HashMap中获取对象并修改其数组
        MyClass retrievedObj = map.get("key");
        retrievedObj.array[0] = 100;

        // 输出HashMap中的对象和修改后的数组
        System.out.println(map.get("key")); // 输出：MyClass@<hashcode>
        System.out.println(map.get("key").array[0]); // 输出：100
    }
}

class MyClass {
    public int[] array;

    public MyClass(int[] array) {
        this.array = array;
    }
}
