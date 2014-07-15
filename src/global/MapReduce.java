package global;

public class MapReduce {
	public static boolean DEBUG = true;
	public static boolean UNITEST = true;

	
	public static class JobTracker {
		public static String jobTrackerServiceName = "JobTracker";
		public static String jobTrackerRegistryIp = "localhost";
		public static int jobTrackerRegistryPort = 1105;
	}
	
	public static class TaskTracker {
		public static String taskTrackerServiceName = "TaskTracker";
		public static int MAX_NUM_MAP_TASK = 3;
		public static int MAX_NUM_REDUCE_TASK = 3;
	}
	
	public static class TasktTracker1 {
		public static String taskTrackerIp = "localhost";
		public static int taskTrackerPort = 1200;
	}
}
