package mapreduce.core;

import global.MapReduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import mapreduce.io.KeyValueCollection;
import mapreduce.io.collector.OutputCollector;
import mapreduce.io.recordreader.RecordReconstructor;
import mapreduce.io.writable.Writable;
import mapreduce.task.PartitionEntry;
import mapreduce.task.ReducerTask;

public class RunReducer <K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable> {
	
	public ReducerTask task;
	public Reducer<K1, V1, K2, V2> reducer;
	
	public static void main(String[] args) {
		
		RunReducer<Writable, Writable, Writable, Writable> rr = new RunReducer<Writable, Writable, Writable, Writable>();
		
		//Configure task
		if (MapReduce.UNITEST) {
			ReducerTask task = new ReducerTask(null, 0, null, null, null);
			rr.task = task;
				
		} else {
			File taskFile = new File(args[0]);
			try {
				FileInputStream fin = new FileInputStream(taskFile);
				ObjectInputStream in = new ObjectInputStream(fin);
				rr.task = (ReducerTask)in.readObject();
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
			}
		}
		
		RecordReconstructor<Writable, Writable> recordReconstructor = 
				new RecordReconstructor<Writable, Writable>();
		//Collect Partition
		if (MapReduce.UNITEST) {
			try {
				for (PartitionEntry taskEntry : rr.task.getEntries()) {
					byte[] buff = null;
					String tmpName = OutputCollector.tmpOutputFileName(rr.task.getJobId(), taskEntry.tid, rr.task.getSEQ());
					buff = taskEntry.taskTrackerStub.transferPartition(tmpName);
					File file = new File(tmpName);
					if (!file.exists()) {
						file.createNewFile();
					}
					FileOutputStream out = new FileOutputStream(file);
					out.write(buff);
					out.flush();
					out.close();
					recordReconstructor.reconstruct(tmpName);
				}
			} catch (RemoteException e) {
				e.printStackTrace();
				System.exit(-6);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			try {
				for (PartitionEntry taskEntry : rr.task.getEntries()) {
					String tmpName = OutputCollector.tmpOutputFileName(rr.task.getJobId(), taskEntry.tid, rr.task.getSEQ());
					recordReconstructor.reconstruct(tmpName);	
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		//Merge Sort
		recordReconstructor.sort();
		recordReconstructor.merge();
		
		OutputCollector<Writable, Writable> output = new OutputCollector<Writable, Writable>(1);
		//Reduce phase
		try {
			rr.reducer = (Reducer<Writable, Writable, Writable, Writable>) rr.task.getTask().getConstructors()[0].newInstance();
			while (recordReconstructor.hasNext()) {
				KeyValueCollection<Writable, Writable> nextLine = recordReconstructor.nextKeyValueCollection();
				rr.reducer.reduce(nextLine.getKey(), nextLine.getValues(), output);
			}
			output.sort();
			output.printOutputCollector();
			output.writeToHDFS(rr.task.getOutputPath());
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
