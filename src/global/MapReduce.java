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
	}
	
	public static class TaskTracker1 {
		public static int httpServerPort = 8000;
	}
}
