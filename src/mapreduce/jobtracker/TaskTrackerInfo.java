package mapreduce.jobtracker;

import java.util.Date;

public class TaskTrackerInfo {
	private String registryIp;
	private int registryPort;
	private int emptyMapSlots;
	private int emptyReduceSlots;
	private Date latestHeartBeat;
	private Status status;
	
	public TaskTrackerInfo(String ip, int port, int mapSlots, int reduceSlots) {
		this.registryIp = ip;
		this.registryPort = port;
		this.emptyMapSlots = mapSlots;
		this.emptyReduceSlots = reduceSlots;
		this.latestHeartBeat = new Date();
		this.status = Status.RUNNING;
	}
	
	public String getIp() {
		return this.registryIp;
	}
	
	public int getPort() {
		return this.registryPort;
	}
	
	public int getMapSlot() {
		return this.emptyMapSlots;
	}
	
	public int getReduceSlot() {
		return this.emptyReduceSlots;
	}
	
	public Date getTimeStamp() {
		return this.latestHeartBeat;
	}
	
	public void updateTimeStamp() {
		this.latestHeartBeat = new Date();
	}
	
	public void setStatus(Status newStat) {
		this.status = newStat;
	}
	
	public Status getStatus() {
		return this.status;
	}
	
	public enum Status {
		RUNNING, FAILED;
	}
}
