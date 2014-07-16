package mapreduce.core;

import example.WordCountReducer;
import global.MapReduce;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

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
		
		try {
			//Configure task
			if (MapReduce.UNITEST) {
				ReducerTask task = new ReducerTask(null, null, 0, WordCountReducer.class, null, "reducerOutput2");
				rr.task = task;	
			} else {
				//TODO: USE RMI to construct task
			}
			
			
			RecordReconstructor<Writable, Writable> recordReconstructor = 
					new RecordReconstructor<Writable, Writable>();
			
			//Collect Partition
			if (MapReduce.UNITEST) {
				for (int i = 0; i < 2; i++) {
					String tmpName = "null-null-" + i;
					recordReconstructor.reconstruct(tmpName);
				}
			} else {
				rr.collectPartition(recordReconstructor);
			}
			
			//Merge Sort & construct Iterator
			recordReconstructor.sort();
			recordReconstructor.merge();
			
			OutputCollector<Writable, Writable> output = new OutputCollector<Writable, Writable>();
			
			//Reduce phase
			rr.reducer = (Reducer<Writable, Writable, Writable, Writable>) rr.task.getTask().getConstructors()[0].newInstance();
			while (recordReconstructor.hasNext()) {
				KeyValueCollection<Writable, Writable> nextLine = recordReconstructor.nextKeyValueCollection();
				rr.reducer.reduce(nextLine.getKey(), nextLine.getValues(), output);
			}
			output.sort();
			output.printOutputCollector();
			output.writeToHDFS(rr.task.getOutputPath());
		} catch (RemoteException e) {
			if (MapReduce.DEBUG) {
				e.printStackTrace();
			}
			System.exit(1);
		} catch (FileNotFoundException e) {
			if (MapReduce.DEBUG) {
				e.printStackTrace();
			}
			System.exit(2);
		} catch (IOException e) {
			if (MapReduce.DEBUG) {
				e.printStackTrace();
			}
			System.exit(3);
		} catch (ClassNotFoundException e) {
			if (MapReduce.DEBUG) {
				e.printStackTrace();
			}
			System.exit(4);
		} catch (InterruptedException e) {
			if (MapReduce.DEBUG) {
				e.printStackTrace();
			}
			System.exit(5);
		} catch (IllegalArgumentException e) {
			if (MapReduce.DEBUG) {
				e.printStackTrace();
			}
			System.exit(6);
		} catch (SecurityException e) {
			if (MapReduce.DEBUG) {
				e.printStackTrace();
			}
			System.exit(7);
		} catch (InstantiationException e) {
			if (MapReduce.DEBUG) {
				e.printStackTrace();
			}
			System.exit(8);
		} catch (IllegalAccessException e) {
			if (MapReduce.DEBUG) {
				e.printStackTrace();
			}
			System.exit(9);
		} catch (InvocationTargetException e) {
			if (MapReduce.DEBUG) {
				e.printStackTrace();
			}
			System.exit(10);
		} catch (NotBoundException e) {
			if (MapReduce.DEBUG) {
				e.printStackTrace();
			}
			System.exit(11);
		}
		
	}
	
	public void collectPartition(RecordReconstructor<K1, V1> recordReconstructor) 
			throws UnknownHostException, IOException, InterruptedException {
		
		List<Thread> threadList = new ArrayList<Thread>();
		

		for (PartitionEntry taskEntry : this.task.getEntries()) {
			
			String tmpName = OutputCollector.tmpOutputFileName(this.task.getJobId(), taskEntry.getTID(), this.task.getSEQ());
			
			File localFile = new File(tmpName);
			FileOutputStream fout = new FileOutputStream(localFile);
			
			Socket soc = new Socket(taskEntry.getHost(), taskEntry.getPort());
			PrintWriter out = new PrintWriter(soc.getOutputStream(), true);
			BufferedInputStream in = new BufferedInputStream(soc.getInputStream());
			
			String request = String.format("%s\n", tmpName);
			out.write(request.toCharArray());
			out.flush();
			
			byte[] buff = new byte[1024];
			int readBytes = 0;
			
			while ((readBytes = in.read(buff)) != -1) {
				fout.write(buff, 0, readBytes);
			}
			in.close();
			out.close();
			fout.close();
			
			Collector<K1, V1> collectorRunnable = new Collector<K1, V1>(recordReconstructor, tmpName);
			Thread collectorThread = new Thread(collectorRunnable);
			collectorThread.start();
			threadList.add(collectorThread);
		}
		
		for (Thread th : threadList) {
			th.wait();
		}
	}
	
	private class Collector<K extends Writable, V extends Writable> implements Runnable {
		
		RecordReconstructor<K, V> rr;
		String fileName;
		
		public Collector (RecordReconstructor<K, V> arg, String file) {
			this.rr = arg;
			this.fileName = file;
		}
		@Override
		public void run() {
			try {
				this.rr.reconstruct(this.fileName);
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
		
	}
}
