package global;

public class MapReduce {
	static boolean DEBUG;
	
	public static class JobTracker {
		public static String jobTrackerServiceName = "JobTracker";
		public static String jobTrackerRegistryIp = "localhost";
		public static int jobTrackerRegistryPort = 1105;
	}
	
	public static class TaskTracker {
		public static String taskTrackerServiceName = "TaskTracker";
	}
}
