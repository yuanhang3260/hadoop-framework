package mapreduce.core;

import global.Hdfs;
import hdfs.DataStructure.HDFSFile;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import mapreduce.io.KeyValue;
import mapreduce.io.Text;
import mapreduce.io.Writable;
import mapreduce.task.Task;

public class RunMapper<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable> {
	public Task task;
	public Mapper<K1, V1, K2, V2> mapper;
	
	/**
	 * The start of the new mapper process
	 * @param args The array of String contain the task file.
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		RunMapper<Writable, Writable, Writable, Writable> rm = new RunMapper<Writable, Writable, Writable, Writable>();
		if (args == null || args.length < 1) {
			Registry nameNodeR = LocateRegistry.getRegistry(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort);
			NameNodeRemoteInterface nameNodeS = (NameNodeRemoteInterface) nameNodeR.lookup(Hdfs.NameNode.nameNodeServiceName);
			HDFSFile file = nameNodeS.open("wordCount");
			Split split = new Split(file,0);
			rm.task = new Task("MapperJob", );
			
		} else {
			File taskFile = new File(args[0]);
			try {
				FileInputStream fin = new FileInputStream(taskFile);
				ObjectInputStream in = new ObjectInputStream(fin);
				rm.task = (Task)in.readObject();
				in.close();
				fin.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				System.exit(-1);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-2);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				System.exit(-3);
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
				System.exit(-4);
			} catch (SecurityException e) {
				e.printStackTrace();
				System.exit(-5);
			} catch (InstantiationException e) {
				e.printStackTrace();
				System.exit(-6);
			} catch (IllegalAccessException e) {
				e.printStackTrace();
				System.exit(-7);
			} catch (InvocationTargetException e) {
				e.printStackTrace();
				System.exit(-8);
			}
		}

		OutputCollector<Writable, Writable> output = new OutputCollector<Writable, Writable>(rm.task.partitionNum);
		RecordReader recordReader = new RecordReader(rm.task.split);
		rm.mapper = (Mapper<Writable, Writable, Writable, Writable>) rm.task.taskClass.getConstructors()[0].newInstance();
		while (recordReader.hasNext()) {
			KeyValue<Text, Text> nextLine = recordReader.nextKeyValue();
			rm.mapper.map(nextLine.getKey(), nextLine.getValue(), output);
		}
		output.writeToLocal();
		
		return;
	}
}
