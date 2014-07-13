package mapreduce;

import global.Hdfs;
import hdfs.DataStructure.HDFSChunk;
import hdfs.DataStructure.HDFSFile;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

import mapreduce.core.Split;
import mapreduce.jobtracker.JobTrackerRemoteInterface;

public class JobClient {
	public static String runJob(JobConf conf) {
		String jobId = null;
		try {
			Registry jobTrackerRegistry = LocateRegistry.getRegistry(Hdfs.JobTracker.jobTrackerRegistryIp, Hdfs.JobTracker.jobTrackerRegistryPort);	
			JobTrackerRemoteInterface jobTrackerStub = (JobTrackerRemoteInterface) jobTrackerRegistry.lookup(Hdfs.JobTracker.jobTrackerServiceName);
			
			Job jobToSubmit = new Job(conf);
			List<Split> splits = splitFile(conf.getInputPath());
			jobToSubmit.setSplit(splits);
			jobId = jobTrackerStub.submitJob(jobToSubmit);
			
			if (Hdfs.DEBUG) {
				System.out.println("DEBUG JobClient.runJob(): Job already submitted, job Id = " + jobId);
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jobId;
	}
	
	public static List<Split> splitFile(String inputFile) throws Exception {
		try {
			Registry nameNodeRegistry = LocateRegistry.getRegistry(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup(Hdfs.NameNode.nameNodeServiceName);
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
		//TODO: cut chunks into approriate splits, use the whole chunk as a split
		//for now
		List<HDFSChunk> chunks = file.getChunkList();
		List<Split> splits = new ArrayList<Split>();
		for (int i = 0; i < chunks.size(); i++) {
			Split split = new Split(file, i);
			splits.add(split);
		}
		return splits;
	}
}
