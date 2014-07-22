package mapreduce.tasktracker;

import global.Hdfs;
import global.MapReduce;
import global.Parser;
import hdfs.io.HDFSFile;
import hdfs.namenode.NameNodeRemoteInterface;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import mapreduce.io.Split;
import mapreduce.jobtracker.JobStatus;
import mapreduce.jobtracker.JobTrackerACK;
import mapreduce.jobtracker.JobTrackerRemoteInterface;
import mapreduce.jobtracker.TaskStatus;
import mapreduce.jobtracker.TaskTrackerInfo;
import mapreduce.jobtracker.TaskTrackerReport;
import mapreduce.jobtracker.WorkStatus;
import mapreduce.message.JarFileEntry;
import mapreduce.message.Job;
import mapreduce.message.MapperTask;
import mapreduce.message.PartitionEntry;
import mapreduce.message.ReducerTask;
import mapreduce.message.Task;


public class JobTrackerSimulator implements JobTrackerRemoteInterface {
	
//	private String taskTrackerIp;
//	private int taskTrackerRegistryPort;
//	private int taskTrackerServerPort;
//	private int counter = 0;

	private List<Task> taskList = Collections.synchronizedList(new LinkedList<Task>());
	
	public static void main(String[] args) throws RemoteException, AlreadyBoundException, NotBoundException, InterruptedException {
		try {
			Parser.hdfsCoreConf();
			Parser.mapreduceCoreConf();
			Parser.mapreduceJobTrackerConf();
			Parser.mapreduceTaskTrackerIndividualConf();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		JobTrackerSimulator jt = new JobTrackerSimulator();
		
		Registry registry = LocateRegistry.createRegistry(MapReduce.Core.JOB_TRACKER_REGISTRY_PORT);
		JobTrackerRemoteInterface jtStub = (JobTrackerRemoteInterface) UnicastRemoteObject.exportObject(jt, 0);
		registry.bind(MapReduce.Core.JOB_TRACKER_SERVICE_NAME, jtStub);
		
		int chunksNum = 8;
		int reducerNum = 1;
		
		
		String jid = "job001";
		
		String tidPrefix = "task";
		int taskCounter = 1;
		
		/* Open file */
		Registry nameNodeR = LocateRegistry.getRegistry(Hdfs.Core.NAME_NODE_IP, Hdfs.Core.NAME_NODE_REGISTRY_PORT);
		NameNodeRemoteInterface nameNodeS = (NameNodeRemoteInterface) nameNodeR.lookup(Hdfs.Core.NAME_NODE_SERVICE_NAME);
		HDFSFile file1 = nameNodeS.open("hello");
		
		Thread.sleep(1000 * 3);
		
//		String mapperClassName = null;
//		String reducerClassName = null;
		
//		try {
//			mapperClassName = jt.loadMapperClass();
//			reducerClassName = jt.loadReducerClass();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (ClassNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		for (int mapperSEQ = 0; mapperSEQ < chunksNum; mapperSEQ++) {
			Split split = new Split(file1, mapperSEQ);
			JarFileEntry jarEntry = new JarFileEntry("128.237.222.59", MapReduce.TaskTracker.Individual.TASK_TRACKER_SERVER_PORT, "/Users/JeremyFu/Dropbox/WordCount.jar");
			jt.taskList.add(new MapperTask(jid, String.format("%s%03d", tidPrefix, taskCounter++), split, "WordCount.WordCountMapper", reducerNum, jarEntry));
		}

		Thread.sleep(1000 * 15);
		
		for (int reducerSEQ = 0; reducerSEQ < reducerNum; reducerSEQ++) {
			PartitionEntry[] partitionEntry = new PartitionEntry[chunksNum];
			for (int i = 0; i < chunksNum; i++) {
				partitionEntry[i] = new PartitionEntry(String.format("%s%03d", tidPrefix, (i+1)), "localhost", MapReduce.TaskTracker.Individual.TASK_TRACKER_SERVER_PORT);
				System.err.println(String.format("%s%03d", tidPrefix, (i+1)));
			}
			JarFileEntry jarEntry = new JarFileEntry("localhost", MapReduce.TaskTracker.Individual.TASK_TRACKER_SERVER_PORT, "/Users/JeremyFu/Dropbox/WordCount.jar");
			jt.taskList.add(new ReducerTask(jid, String.format("%s%03d", tidPrefix, taskCounter++), reducerSEQ, "WordCount.WordCountReducer", partitionEntry, "output-part" + reducerSEQ, jarEntry));

		}	

	}
	
	@Override
	public String submitJob(Job job) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String join(String ip, int port, int serverPort, int mapSlots)
			throws RemoteException {
		
//		this.taskTrackerIp = ip;
//		this.taskTrackerRegistryPort = port;
//		this.taskTrackerServerPort = MapReduce.TaskTracker.Individual.TASK_TRACKER_SERVER_PORT; //TODO: change to join argument
		System.out.println("Target task tracker joins.");
		
		return "TaskTracker-001";
	}

	@Override
	public JobTrackerACK heartBeat(TaskTrackerReport report) {

		report.printReport();
		
		JobTrackerACK rst = new JobTrackerACK(null, null);
		rst.newAddedTasks = new ArrayList<Task>();
		rst.rcvTasks = new ArrayList<TaskStatus>();
		
		while (this.taskList.size() > 0) {
			synchronized (this.taskList) {
				rst.newAddedTasks.add(this.taskList.get(0));
				this.taskList.remove(0);
			}
		}
			
		if (report != null && report.taskStatus != null && report.taskStatus.size() > 0 ) {
			for (TaskStatus rcvTask : report.taskStatus) {
				if (rcvTask.status == WorkStatus.FAILED || rcvTask.status == WorkStatus.SUCCESS) {
					rst.rcvTasks.add(rcvTask);
				}
			}
		}
			
		for (Task task : rst.newAddedTasks) {
			System.out.format("new task: <jid = %s, tid = %s>\n", task.getJobId(), task.getTaskId());
		}
		
		return rst;
	}

	@Override
	public int checkMapProgress(String jobId) throws RemoteException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int checkReduceProgress(String jobId) throws RemoteException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public JobStatus getJobStatus(String jobId) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractMap<String, JobStatus> getAllJobStatus()
			throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void terminateJob(String jobId) throws RemoteException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public AbstractMap<String, TaskTrackerInfo> getTaskTrackerStatus()
			throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractMap<String, Queue<Task>> getScheduleTbl()
			throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}
	
	
//	public String loadMapperClass ()
//			throws IOException, ClassNotFoundException {
//		
//		/* Load Jar file */
//		String jarFilePath = "/Users/JeremyFu/Dropbox/WordCount.jar";
//		JarFile jarFile = new JarFile(jarFilePath);
//		Enumeration<JarEntry> e = jarFile.entries();
//		
//		URL[] urls = { new URL("jar:file:" + jarFilePath +"!/") };
//		ClassLoader cl = URLClassLoader.newInstance(urls);
//		
//		Class<Mapper<Writable, Writable, Writable, Writable>> mapperClass = null;
//		
//		/* Iterate .class files */
//		while (e.hasMoreElements()) {
//            
//			JarEntry je = e.nextElement();
//            
//			if(je.isDirectory() || !je.getName().endsWith(".class")){
//                continue;
//            }
//            
//            String className = je.getName().substring(0, je.getName().length() - 6);
//            className = className.replace('/', '.');
//            if (className.equals("WordCount.WordCountMapper")) {
//            	mapperClass = (Class<Mapper<Writable, Writable, Writable, Writable>>) cl.loadClass(className);
//            }
//        }
//		
//		System.out.println("Mapper class:" + mapperClass.getName());
//		
//		return mapperClass.getName();
//	}
//	
//	
//	public String loadReducerClass ()
//			throws IOException, ClassNotFoundException {
//		
//		/* Load Jar file */
//		String jarFilePath = "/Users/JeremyFu/Dropbox/WordCount.jar";
//		JarFile jarFile = new JarFile(jarFilePath);
//		Enumeration<JarEntry> e = jarFile.entries();
//		
//		URL[] urls = { new URL("jar:file:" + jarFilePath +"!/") };
//		ClassLoader cl = URLClassLoader.newInstance(urls);
//		
//		Class<Reducer<Writable, Writable, Writable, Writable>> reducerClass = null;
//		
//		/* Iterate .class files */
//		while (e.hasMoreElements()) {
//            
//			JarEntry je = e.nextElement();
//            
//			if(je.isDirectory() || !je.getName().endsWith(".class")){
//                continue;
//            }
//            
//            String className = je.getName().substring(0, je.getName().length() - 6);
//            className = className.replace('/', '.');
//            if (className.equals("WordCount.WordCountMapper")) {
//            	reducerClass = (Class<Reducer<Writable, Writable, Writable, Writable>>) cl.loadClass(className);
//            }
//        }
//
//		
//		return reducerClass.getName();
//		
//	}

}
