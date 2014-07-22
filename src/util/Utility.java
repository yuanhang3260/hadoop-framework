package util;

import global.Hdfs;
import global.MapReduce;
import global.Parser;
import hdfs.io.HDFSBufferedOutputStream;
import hdfs.io.HDFSFile;
import hdfs.io.HDFSInputStream;
import hdfs.io.HDFSOutputStream;
import hdfs.namenode.NameNodeRemoteInterface;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import mapreduce.client.JobClient;
import mapreduce.jobtracker.JobStatus;
import mapreduce.jobtracker.JobTrackerRemoteInterface;
import mapreduce.jobtracker.TaskStatus;
import mapreduce.jobtracker.WorkStatus;

public class Utility {
	
	public static void main(String[] args) throws IllegalArgumentException, SecurityException, IOException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		
		try {
			Parser.hdfsCoreConf();
			Parser.mapreduceCoreConf();
			Parser.mapreduceTaskTrackerCommonConf();
			Parser.mapreduceTaskTrackerIndividualConf();
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
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup(Hdfs.Core.NAME_NODE_SERVICE_NAME);
			
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
	
	private static void mapredUtility(String[] args) throws IllegalArgumentException, SecurityException, IOException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		if (args[2].equals("lsjob")) {
			if (args.length != 3) {
				printLsjobUsage();
				return;
			}
			/* list status of all jobs submitted */
			lsjob();
		} else if (args[2].equals("submit")) {
			if (args.length < 8) {
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
	
	private static void submit(String[] args) throws IOException, ClassNotFoundException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, SecurityException, NoSuchMethodException {
		// TODO Auto-generated method stub
		//hadoop mapred submit jar /usr/foo.jar package.ClassName inputPath outputPath
		String type = args[3];
		String jarPath = args[4];
		String mainClassName = args[5];
		String intputPath = args[6];
		String outoutPath = args[7];
		
		JobClient.jarPath = jarPath;
		
		JarFile jarFile = new JarFile(jarPath);
		Enumeration<JarEntry> e = jarFile.entries();
		
		URL[] urls = { new URL("jar:file:" + jarPath +"!/") };
		ClassLoader cl = URLClassLoader.newInstance(urls);
		
		Class<?> mainClass = null;
		
		while (e.hasMoreElements()) {
            
			JarEntry je = e.nextElement();
            
			if(je.isDirectory() || !je.getName().endsWith(".class")){
                continue;
            }
            
            String className = je.getName().substring(0, je.getName().length() - 6);
            className = className.replace('/', '.');
            if (className.equals(mainClassName)) {
            	mainClass =  cl.loadClass(className);
            }
        }
		
	    Method m = mainClass.getMethod("main", new Class[] {String[].class});
	    m.setAccessible(true);
	    
	    int mods = m.getModifiers();
	    
	    if (m.getReturnType() != void.class || !Modifier.isStatic(mods) ||
	        !Modifier.isPublic(mods)) {
	        throw new NoSuchMethodException("main");
	    }
	    
	    String[] newArgs = new String[args.length - 6];
	    newArgs = Arrays.copyOfRange(args, 6, args.length);
	    System.out.println("DEBUG Utility.subit(): newArgs = " + Arrays.toString(newArgs));
	    m.invoke(null, new Object[] { newArgs });
		
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
