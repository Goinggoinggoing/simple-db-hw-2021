import java.io.Serializable;
import java.util.*;

public class MyTest {
    public static void main(String[] args) {
        int [] a = {1,2,3};

        System.out.println(a.getClass());
        System.out.println(Integer.class);

//        ArrayList<Person> nums = new ArrayList<>();
//        nums.add(new Person(1));
//        nums.add(new Person(9));
//        nums.add(new Person(7));
//        nums.add(new Person(0));
//
//        Collections.sort(nums, new Comparator<Person>() {
//            @Override
//            public int compare(Person o1, Person o2) {
//                return o1.getAge() - o2.getAge();
//            }
//        });
        int[] nums = {0,2,5,1,2,3,1,2,3,6,7,8,3,4,1};
        int n = nums.length;
        Stack<Integer> stack = new Stack<Integer>();

        int[] preSmall = new int[n+1];
        int[] nextSmall = new int[n+1];
        long[] presum = new long[n+1];
        for(int i=0;i<n;i++){
            while(!stack.isEmpty() && nums[stack.peek()]>=nums[i]){
                nextSmall[stack.pop()] = i;
            }
//             if(stack.isEmpty()){
//                 preSmall[i] = -1;
//             }else {
////                 System.out.println(i + " "+nums[stack.peek()]+" "+nums[i]);
//                 preSmall[i] = nums[stack.peek()] == nums[i] ? preSmall[stack.peek()]:stack.peek();
//             }

            preSmall[i] = stack.isEmpty() ? -1:stack.peek();
            stack.push(i);
        }

        while(!stack.isEmpty()){
            nextSmall[stack.pop()] = n;
        }

        for(int i=0;i<n;i++){
            System.out.println(nums[i] + " "+preSmall[i]+" "+nextSmall[i]);
        }


        ArrayDeque<Object> objects = new ArrayDeque<>();



    }
    public static void f(int[] a){
        a[1]= 22222;
    }

    private static class Person implements Serializable {
        int age;

        public Person(int age) {
            this.age = age;
        }


        int getAge(){return age;}



    }


}