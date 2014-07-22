package mapreduce.client;

import global.Hdfs;
import global.MapReduce;
import global.Parser;
import hdfs.io.HDFSBufferedOutputStream;
import hdfs.io.HDFSChunk;
import hdfs.io.HDFSFile;
import hdfs.io.HDFSOutputStream;
import hdfs.namenode.NameNodeRemoteInterface;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import mapreduce.io.Split;
import mapreduce.jobtracker.JobStatus;
import mapreduce.jobtracker.JobTrackerRemoteInterface;
import mapreduce.jobtracker.WorkStatus;
import mapreduce.message.Job;

public class JobClient {
	public static String jarPath;
	public static String runJob(JobConf conf) {
		
		try {
			Parser.hdfsCoreConf();
			Parser.mapreduceCoreConf();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("The JobClient cannot read configuration info.\n"
					+ "Please confirm the mapreduce.xml is placed as ./conf/mapreduce.xml.\n"
					+ "The JobClient is shutting down...");
			System.exit(1);
		}
		
		Registry nameNodeR = null;
		NameNodeRemoteInterface nameNodeStub = null;
		String jarFileNameOnHDFS = null;
		if (jarPath != null) {
			try {
				nameNodeR = LocateRegistry.getRegistry(Hdfs.Core.NAME_NODE_IP, Hdfs.Core.NAME_NODE_REGISTRY_PORT);
				nameNodeStub = (NameNodeRemoteInterface) nameNodeR.lookup(Hdfs.Core.NAME_NODE_SERVICE_NAME);
				
				File jarFile = new File(jarPath);
				FileInputStream fileIn = new FileInputStream(jarFile);
				String jarFileName = jarFile.getName();
				jarFileNameOnHDFS = jarFileName + "-" + Inet4Address.getLocalHost().getHostAddress() + "-" + System.currentTimeMillis();
				HDFSFile file = nameNodeStub.create(jarFileNameOnHDFS);
				HDFSBufferedOutputStream bout = new HDFSBufferedOutputStream(file.getOutputStream());
				
				
				byte[] buf = new byte[2048];
				int len = 0;
				while ((len = fileIn.read(buf)) != -1) {
					bout.write(buf, 0, len);
				}
				fileIn.close();
				bout.close();
				
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		String jobId = null;
		try {
			Registry jobTrackerRegistry = LocateRegistry.getRegistry(MapReduce.Core.JOB_TRACKER_IP, MapReduce.Core.JOB_TRACKER_REGISTRY_PORT);	
			JobTrackerRemoteInterface jobTrackerStub = (JobTrackerRemoteInterface) jobTrackerRegistry.lookup(MapReduce.Core.JOB_TRACKER_SERVICE_NAME);
			
			Job jobToSubmit = new Job(conf);
			List<Split> splits = splitFile(conf.getInputPath());
			jobToSubmit.setSplit(splits);
			jobToSubmit.setJarFileEntry(jarFileNameOnHDFS);
			jobId = jobTrackerStub.submitJob(jobToSubmit);
			
			if (Hdfs.Core.DEBUG) {
				System.out.println("DEBUG JobClient.runJob(): Job already submitted, job Id = " + jobId);
			}
			
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd G 'at' HH:mm:ss z");
			int mapTotal = splits.size();
			int reduceTotal = conf.getNumReduceTasks();
			
			int prevMap = -1; 
			int prevReduce = reduceTotal;
			int prevRescheduleNum = 0;
			System.out.println(String.format("INFO: %s Job (Id = %s) start running", dateFormat.format( new Date()), jobId));
			System.out.println("INFO: Number of Map task = " + mapTotal);
			System.out.println("INFO: Number of Reduce task = " + reduceTotal);
			while (true) {
				Thread.sleep(1000 * 3);
//				int numMapIncomplete = jobTrackerStub.checkMapProgress(jobId);
//				int numReduceIncomplete = jobTrackerStub.checkReduceProgress(jobId);
				JobStatus jobStatus = jobTrackerStub.getJobStatus(jobId);
				int numMapIncomplete = jobStatus.mapTaskLeft;
				int numReduceIncomplete = jobStatus.reduceTaskLeft;
				
				if (jobStatus.status == WorkStatus.FAILED) {
					System.out.println(String.format("INFO: Job (Id = %S) failed, existed from execution!", jobId));
					break;
				} else if (jobStatus.status == WorkStatus.TERMINATED) {
					System.out.println(String.format("INFO: Job (Id = %S) terminated", jobId));
					break;
				}
				
				if (jobStatus.rescheduleNum != prevRescheduleNum) {
					System.out.println();
					System.out.println(String.format("INFO: Job (Id = %S) failed, try reschedule now!", jobId));
					prevRescheduleNum = jobStatus.rescheduleNum;
				}
				if (numMapIncomplete != prevMap) {
					System.out.print(String.format("INFO: %s In Map Progress, current progress: %.2f%% ", dateFormat.format(new Date()), 100 * (float)(mapTotal - numMapIncomplete) / (float)mapTotal));
					System.out.println(String.format("(%d / %d)", mapTotal - numMapIncomplete, mapTotal));
					prevMap = numMapIncomplete;
					if (numMapIncomplete == 0) {
						System.out.println(String.format("INFO: Job (Id = %s) enter reduce process", jobId));
					}
				}

				if (numReduceIncomplete != prevReduce) {
					System.out.print(String.format("INFO: %s In Reduce Progress, current progress: %.2f%% ", dateFormat.format(new Date()), 100 * (float)(reduceTotal - numReduceIncomplete) / (float)reduceTotal));
					System.out.println(String.format("(%d / %d)", reduceTotal - numReduceIncomplete, reduceTotal));
					prevReduce = numReduceIncomplete;
				}
				if (numReduceIncomplete == 0) {
					System.out.println(String.format("INFO: %s Job " + jobId + " finished", dateFormat.format(new Date())));
					break;
				}
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			nameNodeStub.delete(jarFileNameOnHDFS);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		return jobId;
	}
	
	public static List<Split> splitFile(String inputFile) throws Exception {
		try {
			Registry nameNodeRegistry = LocateRegistry.getRegistry(Hdfs.Core.NAME_NODE_IP, Hdfs.Core.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup(Hdfs.Core.NAME_NODE_SERVICE_NAME);
			HDFSFile file = nameNodeStub.open(inputFile);
			if (file == null) {
				throw new Exception("Input file " + inputFile + " doesn't exist in HDFS");
			}
			return splitChunks(file);
			
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private static List<Split> splitChunks(HDFSFile file) {
		List<HDFSChunk> chunks = file.getChunkList();
		List<Split> splits = new ArrayList<Split>();
		for (int i = 0; i < chunks.size(); i++) {
			Split split = new Split(file, i);
			splits.add(split);
		}
		return splits;
	}
}
