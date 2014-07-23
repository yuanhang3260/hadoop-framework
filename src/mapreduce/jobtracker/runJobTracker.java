package mapreduce.jobtracker;

import global.Parser;

public class runJobTracker {
	public static void main(String[] args) {
		
		try {
			Parser.hdfsCoreConf();
			Parser.mapreduceCoreConf();
			Parser.mapreduceJobTrackerConf();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("The JobTracker rountine cannot read configuration info.\n"
					+ "Please confirm the mapreduce.xml is placed as ./conf/mapreduce.xml.\n"
					+ "The JobTracker routine is shutting down...");
			System.exit(1);
		}
		
		JobTracker jt = new JobTracker();
		jt.init();
	}
}
