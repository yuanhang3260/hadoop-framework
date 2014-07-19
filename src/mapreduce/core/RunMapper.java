package mapreduce.core;

import example.WordCount.WordCountMapper;
import global.Hdfs;
import global.MapReduce;
import hdfs.DataStructure.HDFSFile;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import mapreduce.io.KeyValue;
import mapreduce.io.Split;
import mapreduce.io.collector.OutputCollector;
import mapreduce.io.recordreader.KeyValueLineRecordReader;
import mapreduce.io.writable.Text;
import mapreduce.io.writable.Writable;
import mapreduce.task.MapperTask;
import mapreduce.tasktracker.TaskTrackerRemoteInterface;

public class RunMapper<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable> {
	public MapperTask task;
	public Mapper<K1, V1, K2, V2> mapper;
	
	/**
	 * The start of the new mapper process
	 * @param args The array of String contain the task file.
	 * @throws RemoteException 
	 * @throws NotBoundException 
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		RunMapper<Writable, Writable, Writable, Writable> rm = new RunMapper<Writable, Writable, Writable, Writable>();
		if (MapReduce.UNITEST) {
			try {
				Registry nameNodeR = LocateRegistry.getRegistry(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort);
				NameNodeRemoteInterface nameNodeS = (NameNodeRemoteInterface) nameNodeR.lookup(Hdfs.Common.NAME_NODE_SERVICE_NAME);
				HDFSFile file = nameNodeS.open("wordCount");
				Split split = new Split(file,0);
				rm.task = new MapperTask("job001", "task001", split, WordCountMapper.class, 2);
				rm.task.setFilePrefix("tmp/TaskTracker-001/Mapper");
				System.out.println("CLASS:" + rm.task.mapperClass + "\t" + WordCountMapper.class);
			} catch (RemoteException e) {
				e.printStackTrace();
				System.exit(-10);
			} catch (NotBoundException e) {
				e.printStackTrace();
				System.exit(-11);
			}
			
		} else {
			try {
				if (MapReduce.DEBUG) {
					System.out.println("DEBUG RunMapper.main(): Try to Retrived task.");
				}
				Registry taskTrackerR = LocateRegistry.getRegistry("localhost", Integer.parseInt(args[0]));
				
				if (MapReduce.DEBUG) {
					System.out.println("DEBUG RunMapper.main(): Formed registry, port:" + Integer.parseInt(args[0]));
				}
				TaskTrackerRemoteInterface taskTrackerS = (TaskTrackerRemoteInterface) taskTrackerR
						.lookup(MapReduce.TaskTracker.taskTrackerServiceName);
				rm.task = (MapperTask) taskTrackerS.getTask(args[1]);
				if (MapReduce.DEBUG) {
					System.out.println("DEBUG RunMapper.main(): Retrived task.");
				}
			} catch (NumberFormatException e) {
				e.printStackTrace();
				System.exit(-1);
			} catch (RemoteException e) {
				e.printStackTrace();
				System.exit(-2);
			} catch (NotBoundException e) {
				e.printStackTrace();
				System.exit(-3);
			}
		}
		
		
		if (MapReduce.TaskTracker.MAPPER_FAULT_TEST) {
			try {
				Thread.sleep(1000 * 2);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.exit(128);
		}
		
		
		try {
			OutputCollector<Writable, Writable> output = new OutputCollector<Writable, Writable>(rm.task);
			KeyValueLineRecordReader recordReader = new KeyValueLineRecordReader(rm.task.split);
			rm.mapper = (Mapper<Writable, Writable, Writable, Writable>) rm.task.mapperClass.getConstructors()[0].newInstance();
			while (recordReader.hasNext()) {
				KeyValue<Text, Text> nextLine = recordReader.nextKeyValue();
				rm.mapper.map(nextLine.getKey(), nextLine.getValue(), output);
			}
			output.sort();
			output.printOutputCollector();
			output.writeToLocal();
			return;
		}  catch (InstantiationException e) {
			e.printStackTrace();
			System.exit(-6);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			System.exit(-7);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			System.exit(-10);
		} catch (SecurityException e) {
			e.printStackTrace();
			System.exit(11);
		} catch (InvocationTargetException e) {
			e.printStackTrace();
			System.exit(-8);
		} 
		catch (IOException e) {
			e.printStackTrace();
			System.exit(-9);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-10);
		}
		
	}
}
