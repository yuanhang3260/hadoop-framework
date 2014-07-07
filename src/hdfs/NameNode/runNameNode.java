package hdfs.NameNode;

import global.Hdfs;

public class runNameNode {
	public static boolean DEBUG = true;
	public static void main(String[] args) {
		NameNode nameNode = new NameNode();
		nameNode.init();
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG runNameNode.main(): NameNode now is running");
		}
	}
}
