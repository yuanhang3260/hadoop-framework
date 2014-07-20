package mapreduce.tasktracker;

import global.Hdfs;
import global.MapReduce;

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
import java.util.Collections;
import java.util.List;

import mapreduce.jobtracker.JobTrackerACK;
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
	
	int slots;
	List<Process> ProcessList;
	List<Task> syncTaskList;
	
	String taskTrackerTmpFolder;
	String taskTrackerMapperFolderName;
	String taskTrackerReducerFolderName;
	
	public static final int BUFF_SIZE = 1024 * 1024;
	
	
	/*------------------ FOR DEBUGGING ----------------*/
	//TODO: REMOVE THE NEXT LINE
	int failureTimes = 0;
	int taskCounter = 0;
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
		

		this.ProcessList = new ArrayList<Process>();
		this.syncTaskList = Collections.synchronizedList(new ArrayList<Task>());

	}
	
	/*------------------Local method-----------------*/
	
	
	public void init() throws RemoteException, UnknownHostException, NotBoundException, IOException {
		
		/* Export and bind TaskTracker */
		Registry registry = LocateRegistry.createRegistry(this.registryPort);
		TaskTrackerRemoteInterface stub = (TaskTrackerRemoteInterface) UnicastRemoteObject.exportObject(this, 0);
		registry.rebind(MapReduce.TaskTracker.Common.TASK_TRACKER_SERVICE_NAME, stub);
		
		/* Locate JobTracker */
		Registry jobTrackerR = LocateRegistry.getRegistry(this.jobTrackerIp, this.jobTrackerPort);
		this.jobTrackerStub = (JobTrackerRemoteInterface)jobTrackerR.lookup(MapReduce.Core.JOB_TRACKER_SERVICE_NAME);
		
		/* Join the MapReduce cluster */
		this.taskTrackerIp = Inet4Address.getLocalHost().getHostAddress();
		this.name = 
			this.jobTrackerStub.join( this.taskTrackerIp,
				this.registryPort, this.slots); //TODO: FIX THE LAST ARGUMENT
		
		/* Finish the TaskTracker temporary folder */
		this.taskTrackerTmpFolder += "/" + this.name;
		File taskTrackerTmpFolderFile = new File (this.taskTrackerTmpFolder);
		if (!taskTrackerTmpFolderFile.exists()) {
			taskTrackerTmpFolderFile.mkdir();
		} 
		
		
		this.taskTrackerMapperFolderName  = this.taskTrackerTmpFolder + "/" + "Mapper";
		this.taskTrackerReducerFolderName = this.taskTrackerTmpFolder + "/" + "Reducer";
		
		File taskTrackerMapperFolder = new File(this.taskTrackerMapperFolderName);
		if (!taskTrackerMapperFolder.exists()) {
			taskTrackerMapperFolder.mkdir();
		} else {
			for (File staleFile : taskTrackerMapperFolder.listFiles()) {
				staleFile.delete();
			}
		}
		
		File taskTrackerReducerFolder = new File(this.taskTrackerReducerFolderName);
		if (!taskTrackerReducerFolder.exists()) {
			taskTrackerReducerFolder.mkdir();
		} else {
			for (File staleFile : taskTrackerMapperFolder.listFiles()) {
				staleFile.delete();
			}
		}
		
//		/* Init tmp file folder*/
//		File tmpFileDirectory = new File(this.taskTrackerTmpFolder);
//		if (!tmpFileDirectory.exists()) {
//			tmpFileDirectory.mkdirs();
//		}

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
		
		if (MapReduce.Core.DEBUG) {
			System.out.println("DEBUG TaskTracker.init(): TaskTracker Initialization succeeds.");
		}
	}
	
	

	
	/*----------------Private Method----------------*/
	
	private void rmTask(String id) {
		synchronized (this.syncTaskList) {
			for (Task task : this.syncTaskList) {
				if ((task.getJobId() + task.getTaskId()).equals(id)) {
					this.syncTaskList.remove(task);
					if (MapReduce.Core.DEBUG) {
						System.out.format("DEBUG TaskTracker.init(): The task(<jid:%s,tid:%s>) is ACKed by JobTracker.\n",
								task.getJobId(), task.getTaskId());
					}
					break;
				}
			}
		}
	}
	
	/*--------------------RMI-----------------------*/
	
	@Override
	public Task getTask(String taskID) throws RemoteException {
		synchronized (this.syncTaskList) {
			for (int i = this.syncTaskList.size() - 1; i >= 0; i--) {
				Task taskRecord = this.syncTaskList.get(i);
				if (String.format("%s-%s", taskRecord.getJobId(), taskRecord.getTaskId()).equals(taskID)) {
					return taskRecord;
				}
			}
		}
		return null;
	}
	
	@Override
	public boolean toFail()  {
		return (failureTimes++ < MapReduce.TaskTracker.Common.REDUCER_FAILURE_TIMES);
	}
	
	
	/*-----------------Nested Class---------------*/
	
	private class HeartBeat implements Runnable {

		@Override
		public void run() {
			try {
				Thread.sleep(MapReduce.TaskTracker.Common.HEART_BEAT_FREQ);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			while (true) {
				
				TaskTrackerReport report = reporter();
				
				JobTrackerACK ack = null;
				
				try {
					ack = TaskTracker.this
							.jobTrackerStub.heartBeat(report); //ack is local variable
				} catch (RemoteException e1) {

					// wait for next heart beat;
					try {//TODO if JobTracker doesn't respond me for XXXX seconds, pull off the TaskTracker
						Thread.sleep(MapReduce.TaskTracker.Common.HEART_BEAT_FREQ);
					} catch (InterruptedException e) { //Do nothing
						if (MapReduce.Core.DEBUG) {
							e.printStackTrace();
						}
					}
					continue;
				}
				
				if (ack == null) {
					continue;
				}
				
//				List<Task> newAddedTasks = ack.newAddedTasks;
				
				for (TaskStatus ackTask : ack.rcvTasks) {
					TaskTracker.this.rmTask(ackTask.jobId + ackTask.taskId);
				}
				
				if (ack.newAddedTasks != null && ack.newAddedTasks.size() > 0) {
					
					
					//TODO: Remove the following line (for fault tolerance test)
					TaskTracker.this.taskCounter += ack.newAddedTasks.size();
					
					//TODO: Remove the following line (for fault tolerance test)
//					if (TaskTracker.this.taskCounter == 5) {
//						System.exit(190);
//					}
					
					for (Task newTask : ack.newAddedTasks) {
						if (newTask instanceof MapperTask) {
							newTask.setFilePrefix(TaskTracker.this.taskTrackerMapperFolderName);
						
						} else if (newTask instanceof ReducerTask) {
							newTask.setFilePrefix(TaskTracker.this.taskTrackerReducerFolderName);
							
						} else {
							//Unknown task: Do nothing;
							ack.newAddedTasks.remove(newTask);
						}
						
						TaskTracker.this.syncTaskList.add(newTask);
					}
					
					/* Allocate a thread to start new added tasks */
					StartTask st = new StartTask(ack.newAddedTasks);
					Thread startTaskTh = new Thread(st);
					startTaskTh.start();
				}
				try {
					Thread.sleep(MapReduce.TaskTracker.Common.HEART_BEAT_FREQ);
				} catch (InterruptedException e) { //Do nothing
					if (MapReduce.Core.DEBUG) {
						e.printStackTrace();
					}
				}
			}
		}
		
		
		private TaskTrackerReport reporter() {  //This is within the heart beat thread
			List<TaskStatus> taskStatusList = new ArrayList<TaskStatus>();
			int runningCounter = 0;
			
			/* Obtain the status for each task */
			synchronized (TaskTracker.this.syncTaskList) {
				for (Task task : TaskTracker.this.syncTaskList) {
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
			}
			
			/* Create report for JobTracker */
			TaskTrackerReport report = new TaskTrackerReport(TaskTracker.this.taskTrackerIp, 
					MapReduce.TaskTracker.Individual.CORE_NUM - runningCounter, taskStatusList);
			
			return report;
		}
		
	}
	
	private class ProcessUpdate implements Runnable {

		@Override
		public void run() {
			while (true) {
				
				Task[] taskArray = null;
				synchronized (TaskTracker.this.syncTaskList) {
					taskArray = TaskTracker.this.syncTaskList.toArray(new Task[0]);
				}
				for (Task task : taskArray) {
					if (task == null) {
						break;
						
					}
					if (task.isRunning()) {
						if (task.getProcRef() != null) {
							try{
								int exitVal = task.getProcRef().exitValue();
								
								if (exitVal == 0) {
									task.commitTask();
									if (MapReduce.Core.DEBUG) {
										String type = (task instanceof MapperTask) ? "Mapper" : "Reducer";
										System.out.format("DEBUG TaskTracker.ProcessUpdate.run():\t"
											+ "Task<jid=%s, tid=%s, type=%s> succeeded.\n",
											task.getJobId(), task.getTaskId(), type);
									}
								} else {
									task.failedTask();
									if (MapReduce.Core.DEBUG) {
										String type = (task instanceof MapperTask) ? "Mapper" : "Reducer";
										System.out.format("DEBUG TaskTracker.ProcessUpdate.run():\t"
											+ "Task<jid=%s, tid=%s, type=%s> failed with CODE %d\n",
											task.getJobId(), task.getTaskId(), type, exitVal);
										
										byte[] errBuff = new byte[1024];
										int c = 0;
										try {
											while ( (c = task.getErrInputStream().read(errBuff)) != -1) {
												System.out.print(new String(errBuff, 0 ,c));
											}
										} catch (IOException e) {
											e.printStackTrace();
										}
										
									}

								}
							} catch (IllegalThreadStateException e) {
								if (Hdfs.Core.DEBUG || MapReduce.Core.DEBUG) {
									if (task instanceof ReducerTask) {
										
										InputStream tmpInputStream = task.getInputStream();
										byte[] buff = new byte[512];
										int c = 0;
										try {
											System.out.println(">>>>>>>>>>>>>>>>>>>>>>WAIT FOR TASK SYSO (" + task.getTaskId() + ")");
											while ((c = tmpInputStream.read(buff)) != -1) {
												System.out.print(new String(buff, 0, c));
											}
											System.out.println("<<<<<<<<<<<<<<<<<<<<<<Finish TASK (" + task.getTaskId() + ")");
										} catch (IOException e1) {
											e1.printStackTrace();
										}
									}
								}
							}
						} 
					}
				}

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
			/* 
			 * Although this task list is shared by StartTask and 
			 * HeartBeat, but HeartBeat won't access it.
			 * 
			 * On the other hand, the those inside tasks would be shared
			 * by main thread.
			 * 
			 * */
			this.taskList = newAddedTaskList;	
		}
		
		@Override
		public void run() {
	
			synchronized (TaskTracker.this.syncTaskList) {
				for (Task task : taskList) {
					
					String taskID = String.format("%s-%s", task.getJobId(), task.getTaskId());
					
					ProcessBuilder pb = null;
					
					if (task instanceof MapperTask) {
						pb = new ProcessBuilder("java", "-cp", "./bin", "mapreduce.core.RunMapper", TaskTracker.this.registryPort + "", taskID);
						System.out.println("TaskTrakcer.StartTask.run(): Start to run mapper");
					} else if (task instanceof ReducerTask) {
						pb = new ProcessBuilder("java", "-cp", "./bin", "mapreduce.core.RunReducer",TaskTracker.this.registryPort + "", taskID);
						System.out.println("TaskTrakcer.StartTask.run(): Start to run reducer");
					}
					
					try {
						Process p = pb.start();
						TaskTracker.this.ProcessList.add(p);
						task.setProcRef(p);
						task.startTask();
				
 					} catch (IOException e) {
						e.printStackTrace();
						task.failedTask();
					}
					
				}
				

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
			BufferedReader in = null;
			BufferedOutputStream out = null;
			FileInputStream fin = null;
			
			try {
				
				/* Set up */
				in =
				        new BufferedReader(
				            new InputStreamReader(soc.getInputStream()));
				
				out =
				        new BufferedOutputStream(soc.getOutputStream());
				
				/* Receive requested file */
				String fileName = in.readLine();
				File file = new File(TaskTracker.this.taskTrackerMapperFolderName +"/" + fileName);
				System.out.println("DEBUG TaskTracker.PartitionResponser.run():\t OPENNING " + TaskTracker.this.taskTrackerMapperFolderName +"/" + fileName);
				fin = new FileInputStream(file);
				byte[] buff = new byte[BUFF_SIZE];
				int readBytes = 0;
				while((readBytes = fin.read(buff)) != -1) {
					out.write(buff, 0, readBytes);
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (out != null) {
						out.flush();
						out.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				try {
					if (in !=null) {
						in.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
		
				if (fin != null) {
					try {
						fin.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
				if (soc != null) {
					try {
						soc.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
			}
			
		}
	}	
}
