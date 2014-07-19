package global;

/* port range: 1099 - 1148*/
public class Hdfs {

	public static boolean DEBUG = true;
	
	public static int WRITE_BUFF_SIZE = 512;
	public static int dataNodePartitionTolerance = 1000 * 60 * 2; //2 min

	public static class NameNode {
		public static String nameNodeServiceName = "NameNode";
		public static String nameNodeRegistryIP = "10.0.0.16";//"128.237.222.59"/*"localhost"*/;
		public static int nameNodeRegistryPort = 1099;
		public static int REPLICA_FACTOR = 2;
		public static int CHUNK_SIZE = 1024 * 8;
	}
	
	public static class Client {
		public static int READ_BUFFER_SIZE = 1024 * 1024;
	}
	
	public static class DataNode {
		public static String dataNodeServiceName = "DataNode";
	}
	
	public static class DataNode1 {
		public static int registryPort = 1100;
	}
	public static class DataNode2 {
		public static int registryPort = 1101;
	}
	public static class DataNode3 {
		public static int registryPort = 1102;
	}
	public static class DataNode4 {
		public static int registryPort = 1103;
	}
	public static class DataNode5 {
		public static int registryPort = 1104;
	}

}
