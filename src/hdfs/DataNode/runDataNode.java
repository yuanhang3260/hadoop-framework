package hdfs.DataNode;

import global.Hdfs;

public class runDataNode {
	public static void main(String[] args) throws InterruptedException {
		DataNode dataNode = new DataNode(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort, Hdfs.DataNode2.registryPort);
		dataNode.init();
		Thread t = new Thread(dataNode);
		t.start();
	}
}
