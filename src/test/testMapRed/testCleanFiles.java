package test.testMapRed;

import java.io.File;

public class testCleanFiles {
	
	public static void main (String[] args) {
		File file = new File("/Users/JeremyFu/Development/workspace/15440Lab3/tmp/128.237.222.59:1200/Reducer");
		for (File staleFile : file.listFiles()) {
			System.out.println(staleFile.getName());
			staleFile.delete();
		}
	}
}
