package mapreduce;

import java.io.Serializable;

import mapreduce.task.JarFileEntry;

public class JobConf implements Serializable {

	private static final long serialVersionUID = 1437439113195756219L;

	private String jobName;
	
	private String inputPath;
	private String outputPath;
	
	private Class<?> mapperClass;
	private Class<?> mapOutputKeyClass;
	private Class<?> mapOutputValueClass;
	
	private Class<?> reducerClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;
	
	private int numMapTasks;
	private int numReduceTasks;
	
	private int priorityLevel;
	private JarFileEntry jarFileEntry;
	
	public void setJarFileEntry(String ip, int port, String path) {
		this.jarFileEntry = new JarFileEntry(ip, port, path);
	}
	
	public JarFileEntry getJarFileEntry() {
		return this.jarFileEntry;
	}
	
	public void setPriority(int level) {
		this.priorityLevel = level;
	}
	
	public int getPriority() {
		return this.priorityLevel;
	}
	
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	
	public String getJobName() {
		return this.jobName;
	}
	
	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}
	
	public String getInputPath() {
		return this.inputPath;
	}
	
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
	
	public String getOutputPath() {
		return this.outputPath;
	}
	
	public void setMapperClass(Class<?> theClass) {
		this.mapperClass = theClass;
	}
	
	public Class<?> getMapper() {
		return this.mapperClass;
	}
	
	public void setMapOutputKeyClass(Class<?> theClass) {
		this.mapOutputKeyClass = theClass;
	}
	
	
	public Class<?> getMapOutputKeyClass() {
		return this.mapOutputKeyClass;
	}
	
	public void setMapOutputValueClass(Class<?> theClass) {
		this.mapOutputValueClass = theClass;
	}
	
	public Class<?> getMapOutputValueClass() {
		return this.mapOutputValueClass;
	}
	
	public void setReducerClass(Class<?> theClass) {
		this.reducerClass = theClass;
	}
	
	public Class<?> getReducerClass() {
		return this.reducerClass;
	}
	
	public void setOutputKeyClass(Class<?> theClass) {
		this.outputKeyClass = theClass;
	}
	
	public Class<?> getOutputKeyClass() {
		return this.outputKeyClass;
	}
	
	public void setOutputValueClass(Class<?> theClass) {
		this.outputValueClass = theClass;
	}
	
	public Class<?> getOutputValueClass() {
		return this.outputValueClass;
	}
	
	public void setNumMapTasks(int num) {
		this.numMapTasks = num;
	}
	
	public int getNumMapTasks() {
		return this.numMapTasks;
	}
	
	public void setNumReduceTasks(int num) {
		this.numReduceTasks = num;
	}
	
	public int getNumReduceTasks() {
		return this.numReduceTasks;
	}
}
