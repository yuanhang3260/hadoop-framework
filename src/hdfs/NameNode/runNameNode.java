package hdfs.NameNode;

import global.Hdfs;

public class runNameNode {
	public static void main(String[] args) {
		NameNode nameNode = new NameNode(Hdfs.NameNode.nameNodeRegistryPort);
		nameNode.init();
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG runNameNode.main(): NameNode now is running");
		}
	}
}
