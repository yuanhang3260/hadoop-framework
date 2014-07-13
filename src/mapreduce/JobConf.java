package mapreduce;

import java.io.Serializable;

public class JobConf implements Serializable {
	private String jobName;
	
	private String inputPath;
	private String outputPath;
	
	private Class<?> mapperClass;
	private Class<?> mapOutputKeyClass;
	private Class<?> mapOutputValueClass;
	
	private Class<?> reducerClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;
	
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
}
