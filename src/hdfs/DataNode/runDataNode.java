package hdfs.DataNode;

public class runDataNode {
	public static void main(String[] args) throws InterruptedException {
		DataNode dataNode = new DataNode("128.237.222.59", 1099);
		dataNode.init();
		Thread t = new Thread(dataNode);
		t.start();
	}
}
