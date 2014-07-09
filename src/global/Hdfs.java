package global;


public class Hdfs {
	public static boolean DEBUG = false;

	public static class NameNode {
		public static String nameNodeRegistryIP = "localhost";
		public static int nameNodeRegistryPort = 1099;
		public static int REPLICA_FACTOR = 1;
		public static long CHUNK_SIZE = 1024 * 512;
	}
	
	public static class Client {
		public static int READ_BUFFER_SIZE = 93;
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
}
