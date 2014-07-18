package mapreduce.jobtracker;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mapreduce.task.Task;

public class TaskTrackerInfo {
	private String registryIp;
	private int registryPort;
	//private int emptyMapSlots;
	//private int emptyReduceSlots;
	private long latestHeartBeat;
	private Status status;
	/* tasks defined related with this TaskTracker are:
	 * 1. tasks currently running on this TaskTracker
	 * 2. tasks completed on this TaskTracker */
	private Set<String> relateTasks;
	
	public TaskTrackerInfo(String ip, int port/*, int mapSlots, int reduceSlots*/) {
		this.registryIp = ip;
		this.registryPort = port;
		//this.emptyMapSlots = mapSlots;
		//this.emptyReduceSlots = reduceSlots;
		this.latestHeartBeat = System.currentTimeMillis();
		this.status = Status.RUNNING;
		this.relateTasks = new HashSet<String>();
	}
	
	public void addTask(List<Task> tasks) {
		for (Task task : tasks) {
			String taskId = task.getTaskId();
			if (taskId == null) {
				System.out.println("DEBUG: taskId null");
			}
			this.relateTasks.add(taskId);
		}
	}
	
	public void removeTask(String taskId) {
		this.relateTasks.remove(taskId);
	}
	
	public Set<String> getRelatedTasks() {
		return this.relateTasks;
	}
	
	public String getIp() {
		return this.registryIp;
	}
	
	public int getPort() {
		return this.registryPort;
	}
	
//	public int getMapSlot() {
//		return this.emptyMapSlots;
//	}
//	
//	public int getReduceSlot() {
//		return this.emptyReduceSlots;
//	}
	
	public long getTimeStamp() {
		return this.latestHeartBeat;
	}
	
	public void updateTimeStamp() {
		this.latestHeartBeat = System.currentTimeMillis();
	}
	
	public void setStatus(Status newStat) {
		this.status = newStat;
	}
	
	public Status getStatus() {
		return this.status;
	}
	
	public void disable() {
		this.status = Status.FAILED;
	}
	
	public void enable() {
		this.status = Status.RUNNING;
	}
	
	public enum Status {
		RUNNING, FAILED;
	}
}
