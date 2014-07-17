package mapreduce.tasktracker;

import global.MapReduce;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import mapreduce.jobtracker.JobTrackerRemoteInterface;
import mapreduce.jobtracker.TaskStatus;
import mapreduce.jobtracker.TaskTrackerReport;
import mapreduce.jobtracker.WorkStatus;
import mapreduce.task.MapperTask;
import mapreduce.task.ReducerTask;
import mapreduce.task.Task;

public class TaskTracker implements TaskTrackerRemoteInterface {
	
	String name;
	
	String taskTrackerIp;
	int registryPort;
	int serverPort;
	String jobTrackerIp;
	int jobTrackerPort;
	JobTrackerRemoteInterface jobTrackerStub;
	
	List<Task> taskList;
	int slots;
	List<Process> ProcessList;
	Collection<Task> taskListMutex;
	
	String taskTrackerTmpFolder;
	String taskTrackerMapperFolder;
	String taskTrackerReducerFolder;
	
	/*------------------ Constructor -----------------*/
	/**
	 * 
	 * @param jobTrackerIp The JobTracker IP
	 * @param jobTrackerPort The JobTracker registry port
	 * @param registryPort The registry port to export TaskTracker
	 * itself;
	 * @param serverPort The server port that TaskTracker accepts
	 * partition downloading request
	 * @param nodeTmpFolder The temporary folder on this node
	 */
	public TaskTracker(String jobTrackerIp, int jobTrackerPort, 
			int registryPort, int serverPort, String nodeTmpFolder) {
		
		this.registryPort = registryPort;
		this.serverPort = serverPort;
		this.jobTrackerIp = jobTrackerIp;
		this.jobTrackerPort = jobTrackerPort;
		this.taskTrackerTmpFolder = nodeTmpFolder;
		File tmpFolder = new File (this.taskTrackerTmpFolder);
		if (!tmpFolder.exists()) {
			tmpFolder.mkdir();
		}
		
		this.taskList = new ArrayList<Task>();
		this.ProcessList = new ArrayList<Process>();
		this.taskListMutex = Collections.synchronizedCollection(taskList);

	}
	
	/*------------------Local method-----------------*/
	
	
	public void init() throws RemoteException, UnknownHostException, NotBoundException, IOException {
		
		/* Export and bind TaskTracker */
		Registry registry = LocateRegistry.createRegistry(this.registryPort);
		TaskTrackerRemoteInterface stub = (TaskTrackerRemoteInterface) UnicastRemoteObject.exportObject(this, 0);
		registry.rebind(MapReduce.TaskTracker.taskTrackerServiceName, stub);
		
		/* Locate JobTracker */
		Registry jobTrackerR = LocateRegistry.getRegistry(this.jobTrackerIp, this.jobTrackerPort);
		this.jobTrackerStub = (JobTrackerRemoteInterface)jobTrackerR.lookup(MapReduce.JobTracker.jobTrackerServiceName);
		
		/* Join the MapReduce cluster */
		this.taskTrackerIp = Inet4Address.getLocalHost().getHostAddress();
		this.name = 
			this.jobTrackerStub.join( this.taskTrackerIp,
				this.registryPort, this.slots, 0); //TODO: FIX THE LAST ARGUMENT
		
		/* Finish the TaskTracker temporary folder */
		this.taskTrackerTmpFolder += "/" + this.name;
		File taskTrackerTmpFolderFile = new File (this.taskTrackerTmpFolder);
		if (!taskTrackerTmpFolderFile.exists()) {
			taskTrackerTmpFolderFile.mkdir();
		} 
		
		
		this.taskTrackerMapperFolder  = this.taskTrackerTmpFolder + "/" + "Mapper";
		this.taskTrackerReducerFolder = this.taskTrackerTmpFolder + "/" + "Reducer";
		
		File taskTrackerMapperFolderFile = new File(this.taskTrackerMapperFolder);
		if (!taskTrackerMapperFolderFile.exists()) {
			taskTrackerMapperFolderFile.mkdir();
		} else {
			for (File staleFile : taskTrackerMapperFolderFile.listFiles()) {
				staleFile.delete();
			}
		}
		
		File taskTrackerReducerFolderFile = new File(this.taskTrackerReducerFolder);
		if (!taskTrackerReducerFolderFile.exists()) {
			taskTrackerReducerFolderFile.mkdir();
		} else {
			for (File staleFile : taskTrackerMapperFolderFile.listFiles()) {
				staleFile.delete();
			}
		}
		
		/* Init tmp file folder*/
		File tmpFileDirectory = new File(this.taskTrackerTmpFolder);
		if (!tmpFileDirectory.exists()) {
			tmpFileDirectory.mkdirs();
		}

		/* Start Heart beat */
		HeartBeat hb = new HeartBeat();
		Thread heartBeatTh = new Thread(hb);
		heartBeatTh.start();
		
		/* Start process update routine */
		ProcessUpdate pu = new ProcessUpdate();
		Thread processUpdateTh = new Thread(pu);
		processUpdateTh.start();
		
		/* Start the partition transferring server */
		PartitionServer sv = new PartitionServer(this.serverPort);
		Thread serverTh = new Thread(sv);
		serverTh.start();
		
		if (MapReduce.DEBUG) {
			System.out.println("DEBUG TaskTracker.init(): TaskTracker Initialization succeeds.");
		}
	}
	
	

	
	/*----------------Private Method----------------*/
	

	
	/*--------------------RMI-----------------------*/
	
	@Override
	public Task getTask(String taskID) throws RemoteException {
//		synchronized (this.taskListMutex) {
			for (int i = this.taskList.size() - 1; i >= 0; i--) {
				Task taskRecord = this.taskList.get(i);
				if (String.format("%s-%s", taskRecord.getJobId(), taskRecord.getTaskId()).equals(taskID)) {
//					Task rst = new Task(taskRecord);
					return taskRecord;
				}
			}
			return null;
//		}
	}
	
	
	/*-----------------Nested Class---------------*/
	
	private class HeartBeat implements Runnable {

		@Override
		public void run() {
			try {
				Thread.sleep(MapReduce.TaskTracker.HEART_BEAT_FREQ);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			while (true) {
				
				TaskTrackerReport report = reporter();
				
				
				List<Task> newAddedTasks = null;
				try {
					newAddedTasks = TaskTracker.this.jobTrackerStub.heartBeat(report);
				} catch (RemoteException e1) {
					e1.printStackTrace();
				}
				
				if (newAddedTasks != null && newAddedTasks.size() > 0) {
					synchronized (TaskTracker.this.taskListMutex) {
						for (Task newTask : newAddedTasks) {
							if (newTask instanceof MapperTask) {
								newTask.setFilePrefix(TaskTracker.this.taskTrackerMapperFolder);
								System.out.println("TaskTracker.HeartBeat.run(): The new task is a Mapper");
							} else if (newTask instanceof ReducerTask) {
								newTask.setFilePrefix(TaskTracker.this.taskTrackerReducerFolder);
								System.out.println("TaskTracker.HeartBeat.run(): The new task is a Reducer");
							}
							TaskTracker.this.taskList.add(newTask);
						}
					}
					/* Allocate a thread to start new added tasks */
					StartTask st = new StartTask(newAddedTasks);
					Thread startTaskTh = new Thread(st);
					startTaskTh.start();
				}
				try {
					Thread.sleep(MapReduce.TaskTracker.HEART_BEAT_FREQ);
				} catch (InterruptedException e) { //Do nothing
					if (MapReduce.DEBUG) {
						e.printStackTrace();
					}
				}
			}
		}
		
		private TaskTrackerReport reporter() {
			List<TaskStatus> taskStatusList = new ArrayList<TaskStatus>();
			int runningCounter = 0;
			for (Task task : TaskTracker.this.taskList) {
				if (task.getTaskStatus() == WorkStatus.RUNNING) {
					runningCounter++;
				}
				taskStatusList.add(
						new TaskStatus(task.getJobId(), 
								task.getTaskId(), 
								task.getTaskStatus(), 
								TaskTracker.this.taskTrackerIp, 
								TaskTracker.this.registryPort));
			}
			
			TaskTrackerReport report = new TaskTrackerReport(TaskTracker.this.taskTrackerIp, 
					MapReduce.TasktTracker1.CORE_NUM - runningCounter, taskStatusList);
			return report;
		}
		
	}
	
	private class ProcessUpdate implements Runnable {

		@Override
		public void run() {
			while (true) {
//				synchronized (TaskTracker.this.taskListMutex) { //TODO: PUT BACK sync
					for (Task task : TaskTracker.this.taskList) {
						if (task.isRunning()) {
							try {
								if (task.getProcRef() == null) {
									task.printInfo();
								} else {
									int exitVal = task.getProcRef().exitValue();
									if (exitVal == 0) {
										task.commitTask();
									} else {
										task.failedTask();
									}
								}
							} catch (IllegalThreadStateException e) {
								//DO NOTHING
							}
						}
					}
//				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
		}
		
	}
	
	private class StartTask implements Runnable {
		
		private List<Task> taskList;
		
		public StartTask(List<Task> newAddedTaskList) {
			this.taskList = newAddedTaskList;
		}
		
		@Override
		public void run() {
			String portString = "" + TaskTracker.this.registryPort;
//			synchronized (TaskTracker.this.taskListMutex) {
				for (Task task : taskList) {
					String taskID = String.format("%s-%s", task.getJobId(), task.getTaskId());
					ProcessBuilder pb = null;
					if (task instanceof MapperTask) {
						pb = new ProcessBuilder("java", "-cp", "./bin", "mapreduce.core.RunMapper", portString, taskID);
						System.out.println("TaskTrakcer.StartTask.run(): Start to run mapper");
					} else if (task instanceof ReducerTask) {
						pb = new ProcessBuilder("java", "-cp", "./bin", "mapreduce.core.RunReducer",portString, taskID);
						System.out.println("TaskTrakcer.StartTask.run(): Start to run reducer");
					}
					try {
						Process p = pb.start();
						TaskTracker.this.ProcessList.add(p);
						task.setProcRef(p);
						task.startTask();
//						TaskTracker.this.slots--;
//						TODO: DELETE following block
						if (task instanceof ReducerTask) {
							System.out.println("The task is running");
							InputStream in = p.getInputStream();
							BufferedInputStream bin = new BufferedInputStream(in);
							InputStream errin = p.getErrorStream();
							
							byte[] buff = new byte[1024];
							int c = 0;
							while((c = bin.read(buff)) != -1){
								System.out.print(new String(buff, 0, c));
							}
							while ((c = errin.read(buff)) != -1) {
								System.out.println(new String(buff, 0, c));
							}
							
							
							try {
								p.waitFor();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							System.out.println("The reducuer ends with CODE:" + p.exitValue());
						}						
 					} catch (IOException e) {
						e.printStackTrace();
						task.failedTask();
					}
					
//				}
				

			}
			
		}	
	}
	
	private class PartitionServer implements Runnable {
		
		ServerSocket serverSoc;
		
		public PartitionServer(int port) throws IOException {
			this.serverSoc = new ServerSocket(port);
		}

		@Override
		public void run() {
			while (true) {
				try {
					Socket soc = this.serverSoc.accept();
					PartitionResponser pr = new PartitionResponser(soc);
					Thread prTh = new Thread(pr);
					prTh.start();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
		}
	}
	
	private class PartitionResponser implements Runnable {
		
		Socket soc;
		
		public PartitionResponser (Socket soc) {
			this.soc = soc;
		}
		
		@Override
		public void run() {
			try {
				
				/* Set up */
				BufferedReader in =
				        new BufferedReader(
				            new InputStreamReader(soc.getInputStream()));
				
				BufferedOutputStream out =
				        new BufferedOutputStream(soc.getOutputStream());
				
				/* Receive requested file */
				String fileName = in.readLine();
				File file = new File(TaskTracker.this.taskTrackerMapperFolder +"/" + fileName);
				FileInputStream fin = new FileInputStream(file);
				byte[] buff = new byte[MapReduce.TaskTracker.BUFF_SIZE];
				int readBytes = 0;
				while((readBytes = fin.read(buff)) != -1) {
					out.write(buff, 0, readBytes);
				}
				out.flush();
				out.close();
				in.close();
				fin.close();
				soc.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
	}	
}
