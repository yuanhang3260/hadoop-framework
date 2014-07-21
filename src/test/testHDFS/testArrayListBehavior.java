package test.testHDFS;

import java.util.ArrayList;
import java.util.List;

public class testArrayListBehavior {
	
	public static void main(String[] args) {
		
		List<Integer> list = new ArrayList<Integer>();
		list.add(0);
		list.add(1);
		list.add(2);
		list.add(3);
		
		int i = 0;
		System.out.format("List<%d>:\t%d\n", i, list.get(i));
		i++;
		System.out.format("List<%d>:\t%d\n", i, list.get(i));
		list.remove(i);
		System.out.format("Remove %d\n", i);
		System.out.format("List<%d>:\t%d\n", i, list.get(i));
	}
}
