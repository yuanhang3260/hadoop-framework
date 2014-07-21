package util;

import global.Hdfs;
import global.MapReduce;
import global.Parser;
import hdfs.DataStructure.HDFSFile;
import hdfs.IO.HDFSBufferedOutputStream;
import hdfs.IO.HDFSInputStream;
import hdfs.IO.HDFSOutputStream;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Set;

import mapreduce.JobClient;
import mapreduce.JobConf;
import mapreduce.jobtracker.JobStatus;
import mapreduce.jobtracker.JobTrackerRemoteInterface;
import mapreduce.jobtracker.TaskStatus;
import mapreduce.jobtracker.WorkStatus;

public class Utility {
	
	public static void main(String[] args) {
		
		try {
			Parser.hdfsCoreConf();
			Parser.mapreduceCoreConf();
		} catch (Exception e) {
			
			e.printStackTrace();
			
			System.err.println("The Utility cannot read configuration info.\n"
					+ "Please confirm the hdfs.xml is placed as ./conf/hdfs.xml.\n"
					+ "The Utility is shutting down...");
			
			System.exit(1);
		} 
		
		if (args.length < 2) {
			printUsage();
			return;
		}
		
		if (!args[0].equals("hadoop")) {
			printUsage();
			return;
		}
		
		if (args[1].equals("put")) {
			if (args.length < 4) {
				printPutUsage();
				return;
			}
			String localFilePath = args[2];
			String hdfsFilePath  = args[3];
			putToHDFS(localFilePath, hdfsFilePath);
		} else if (args[1].equals("get")) {
			if (args.length < 4) {
				printGetUsage();
				return;
			}
			String localFilePath = args[3];
			String hdfsFilePath  = args[2];
			getFromHDFS(hdfsFilePath, localFilePath);
		} else if (args[1].equals("rm")) {
			if (args.length < 3) {
				printRmUsage();
				return;
			}
			String hdfsFilePath = args[2];
			removeFromHDFS(hdfsFilePath);
		} else if (args[1].equals("ls")) {
			listFiles();
			return;
		} else if (args[1].equals("mapred")) {
			mapredUtility(args);
		} else {
			printUsage();
			return;
		}
	}
	
	private static void getFromHDFS(String hdfsFilePath, String localFilePath) {
		try {
			Registry nameNodeRegistry = LocateRegistry.getRegistry(Hdfs.Core.NAME_NODE_IP, Hdfs.Core.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
			
			HDFSFile file = nameNodeStub.open(hdfsFilePath);
			if (file == null) {
				System.out.println("Error! HDFS file does not exists");
				System.exit(-1);
			}
			HDFSInputStream in = file.getInputStream();
			int c = 0;
			int buff_len = Hdfs.Core.READ_BUFF_SIZE;
			byte[] buff = new byte[buff_len];
			File newFile = new File(localFilePath);
			FileOutputStream out = null;
			try {
				out =  new FileOutputStream(newFile);
			} catch (FileNotFoundException e) {
				try {
					newFile.createNewFile();
					out =  new FileOutputStream(newFile);
				} catch (IOException e1) {
					System.out.println("Error! Failed to put file to HDFS.");
					System.exit(-1);
				}
			}
			int counter = 0;
			while ((c = in.read(buff)) != 0) {
				out.write(buff, 0, c);
				counter += c;
			}
			out.close();
			System.out.println("TOTALLY READ: " + counter);
		} catch (RemoteException e) {
			System.out.println("Error! Failed to put file to HDFS.");
			System.exit(-1);
		} catch (NotBoundException e) {
			System.out.println("Error! Failed to put file to HDFS.");
			System.exit(-1);
		} catch (IOException e) {
			System.out.println("Error! Failed to put file to HDFS.");
			System.exit(-1);
		}
	}
	
	private static void putToHDFS(String localFilePath, String hdfsFilePath) {
		File newFile = new File(localFilePath);
		if (!newFile.exists()) {
			System.out.println("Error! Local file does not exists");
			System.exit(-1);
		}
		
		byte[] buff = new byte[Hdfs.Core.WRITE_BUFF_SIZE];
		try {
			FileInputStream in = new FileInputStream(newFile);
			int c = 0;
			Registry nameNodeRegistry = LocateRegistry.getRegistry(Hdfs.Core.NAME_NODE_IP, Hdfs.Core.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
			
			HDFSFile file = nameNodeStub.create(hdfsFilePath);
			HDFSOutputStream out = file.getOutputStream();
			HDFSBufferedOutputStream bout = new HDFSBufferedOutputStream(out);
			
			while ((c = in.read(buff)) != -1) {
				bout.write(buff, 0, c);
			}
			
			bout.close();
			in.close();
		} catch (Exception e) {
			System.out.println("Error! Failed to put file to HDFS.");
			System.exit(-1);
		}
	}
	
	private static void removeFromHDFS(String path) {
		try {
			Registry nameNodeRegistry = LocateRegistry.getRegistry(Hdfs.Core.NAME_NODE_IP, Hdfs.Core.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup(Hdfs.Core.NAME_NODE_SERVICE_NAME);
			nameNodeStub.delete(path);
		} catch (RemoteException e){
			System.out.println("Remove operation may failed");
			System.exit(-1);
		} catch (NotBoundException e) {
			System.out.println("Error! Cannot find name node.");
			System.exit(-1);
		} catch (IOException e) {
			System.out.println("Exception! Some data node may lose connection. The file is removed.");
			System.exit(-1);
		}
	}
	
	private static void listFiles() {
		try {
			Registry nameNodeRegistry = LocateRegistry.getRegistry(Hdfs.Core.NAME_NODE_IP, Hdfs.Core.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup(Hdfs.Core.NAME_NODE_SERVICE_NAME);
			ArrayList<String> rst = nameNodeStub.listFiles();
			int i = 1;
			for (String fileName : rst) {
				System.out.format("%d\t%s\n", i, fileName);
				i++;
			}
			if (rst.size() < 1) {
				System.out.println("No files on HDFS");
			}
		} catch (RemoteException e){
			System.out.println("Error! Cannot find name node.");
			System.exit(-1);
		} catch (NotBoundException e) {
			System.out.println("Error! Cannot find name node.");
			System.exit(-1);
		}
	}
	
	public static void printUsage() {
		System.out.println("Usage:\thadoop <op> \n<op>:\n\tput\n\tget");
	}
	
	private static void printPutUsage() {
		System.out.println("Usage:\thadoop\tput\t<src file name>\t<dst file name>");
	}
	
	private static void printGetUsage() {
		System.out.println("Usage:\thadoop\tget\t<dst file name>\t<src file name>");
	}
	
	private static void printRmUsage() {
		System.out.println("Usage:\thadoop\tdelete\t<obj file name>");
	}
	
	/* -------------------- MapReduce utility area --------------------- */
	
	private static void mapredUtility(String[] args) {
		if (args[2].equals("lsjob")) {
			if (args.length != 3) {
				printLsjobUsage();
				return;
			}
			/* list status of all jobs submitted */
			lsjob();
		} else if (args[2].equals("submit")) {
			if (args.length != 11) {
				printSubmitUsage();
				return;
			}
			/* submit job to Job Tracker */
			submit(args);
		} else if (args[2].equals("kill")) {
			if (args.length != 4) {
				printKillUsage();
				return;
			}
			
			kill(args);
		}
	}

	private static void lsjob() {
		Registry jobTrackerRegistry;
		try {
			jobTrackerRegistry = LocateRegistry.getRegistry(MapReduce.Core.JOB_TRACKER_IP, 
															MapReduce.Core.JOB_TRACKER_REGISTRY_PORT);
			
			JobTrackerRemoteInterface jobTrackerStub = 
					(JobTrackerRemoteInterface) jobTrackerRegistry.lookup(MapReduce.Core.JOB_TRACKER_SERVICE_NAME);
			
			AbstractMap<String, JobStatus> jobStatusTbl = jobTrackerStub.getAllJobStatus();
			Set<String> jobIds = jobStatusTbl.keySet();
			System.out.println("Number of jobs on Job Tracker in total: " + jobIds.size());
			for (String jobId : jobIds) {
				lsonejob(jobId, jobStatusTbl);
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}	
	}
	
	private static void lsonejob(String jobId, AbstractMap<String, JobStatus> jobStatusTbl) {
		System.out.println("--------------------------------");
		System.out.println("Job Name: " + jobStatusTbl.get(jobId).jobName);
		System.out.println("Job ID: " + jobId);
		System.out.println("Number of mapper  tasks in total: " + jobStatusTbl.get(jobId).mapTaskTotal);
		System.out.println("Number of reducer tasks in total: " + jobStatusTbl.get(jobId).reduceTaskTotal);
		WorkStatus status = jobStatusTbl.get(jobId).status;
		System.out.println("Status: " + status);
		if (status == WorkStatus.RUNNING) {
			/* print out all mapper tasks status */
			AbstractMap<String, TaskStatus> mapperTbl = jobStatusTbl.get(jobId).mapperStatusTbl;
			Set<String> mapTaskIds = mapperTbl.keySet();
			for (String mapTaskId : mapTaskIds) {
				String ip = mapperTbl.get(mapTaskId).taskTrackerIp == null ? "not assigned yet" : mapperTbl.get(mapTaskId).taskTrackerIp;
				System.out.println("Mapper  task (id = " + mapTaskId + ") status: " + mapperTbl.get(mapTaskId).status + ", Task Tracker " + ip);
			}
			
			/* print out all reducer tasks status */
			AbstractMap<String, TaskStatus> reducerTbl = jobStatusTbl.get(jobId).reducerStatusTbl;
			Set<String> reduceTaskIds = reducerTbl.keySet();
			if (reduceTaskIds.size() == 0) {
				System.out.println("Reducer tasks will run after mapper tasks complete");
			}
			for (String reduceTaskId : reduceTaskIds) {
				String ip = reducerTbl.get(reduceTaskId).taskTrackerIp == null ? "not assigned yet" : reducerTbl.get(reduceTaskId).taskTrackerIp;
				System.out.println("Reducer task (id = " + reduceTaskId + ") status: " + reducerTbl.get(reduceTaskId).status + ", Task Tracker " + ip);
			}
		}
	}
	
	private static void submit(String[] args) {
		// TODO Auto-generated method stub
		String jobName = args[3];
		String jarFilePath = args[4];
		String mapperClassName = args[5];
		String reducerClassName = args[6];
		String fileIn = args[7];
		String fileOut = args[8];
		int partitionNum = 1;
		try {
			partitionNum = Integer.parseInt(args[9]);
		} catch (NumberFormatException e) {
			System.out.println("Exception: NumOfReducer should be an integer ");
			printSubmitUsage();
			return;
		}
		int priority = 0;
		try {
			priority = Integer.parseInt(args[10]);
		} catch (NumberFormatException e) {
			System.out.println("Exception: JobPriority shoud be an interger");
			printSubmitUsage();
			return;
		}
		Class<?> mapperClass = null;
		Class<?> reducerClass = null;
		try {
			mapperClass = Class.forName(mapperClassName);
		} catch (ClassNotFoundException e) {
			System.out.println("Exception: Mapper class not found");
			printSubmitUsage();
			return;
		}
		
		try {
			reducerClass = Class.forName(reducerClassName);
		} catch (ClassNotFoundException e) {
			System.out.println("Exception: Reducer class not found");
			printSubmitUsage();
			return;
		}
		
		JobConf conf = new JobConf();
		conf.setJobName(jobName);
		conf.setJarFileEntry(Inet4Address.getLocalHost().getHostAddress(), , path);
		conf.setMapperClass(mapperClass);
		conf.setReducerClass(reducerClass);
		conf.setInputPath(fileIn);
		conf.setOutputPath(fileOut);
		conf.setNumReduceTasks(partitionNum);
		conf.setPriority(priority);
		JobClient.runJob(conf);
		
	}
	
	private static void kill(String[] args) {
		String jobId = args[3];
		Registry jobTrackerRegistry = null;
		try {
			jobTrackerRegistry = LocateRegistry.getRegistry(MapReduce.Core.JOB_TRACKER_IP, 
					MapReduce.Core.JOB_TRACKER_REGISTRY_PORT);
			
			JobTrackerRemoteInterface jobTrackerStub = 
					(JobTrackerRemoteInterface) jobTrackerRegistry.lookup(MapReduce.Core.JOB_TRACKER_SERVICE_NAME);
			
			jobTrackerStub.terminateJob(jobId);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
	
	private static void printKillUsage() {
		System.out.println("kill:\tKill a job\nUsage: hadoop mapred kill <jobId>");
	}
	
	private static void printSubmitUsage() {
		System.out.println("submit:\tSubmit a mapreduce job to JobTracker\nUsage: hadoop mapred submit <JobName> <JarFilePath> <MapperClassName> <ReducerClassName> <InputFilePath> <OutputFilePath> <NumOfReducer> <JobPriority>");
	}
	
	private static void printLsjobUsage() {
		System.out.println("lsjob:\tList all jobs' status on Job Tracker\nUsage:\thadoop\tmapred\tlsjob");
	}
}
