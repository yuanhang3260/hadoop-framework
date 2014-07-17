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
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

import mapreduce.io.KeyValueCollection;
import mapreduce.io.collector.OutputCollector;
import mapreduce.io.recordreader.RecordReconstructor;
import mapreduce.io.writable.Writable;
import mapreduce.task.MapperTask;
import mapreduce.task.PartitionEntry;
import mapreduce.task.ReducerTask;
import mapreduce.tasktracker.TaskTrackerRemoteInterface;

public class RunReducer <K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable> {
	
	public ReducerTask task;
	public Reducer<K1, V1, K2, V2> reducer;
	
	public static void main(String[] args) {
		
		RunReducer<Writable, Writable, Writable, Writable> rr = new RunReducer<Writable, Writable, Writable, Writable>();
		
		try {
			//Configure task
			if (MapReduce.UNITEST) {
				ReducerTask task = new ReducerTask("job001", "task002", 0, WordCountReducer.class, null, "output");
				rr.task = task;	
			} else {
				Registry taskTrackerR = LocateRegistry.getRegistry("localhost", Integer.parseInt(args[0]));
				TaskTrackerRemoteInterface taskTrackerS = (TaskTrackerRemoteInterface) taskTrackerR
						.lookup(MapReduce.TaskTracker.taskTrackerServiceName);
				rr.task = (ReducerTask) taskTrackerS.getTask(args[1]);
			}
			
			RecordReconstructor<Writable, Writable> recordReconstructor = 
					new RecordReconstructor<Writable, Writable>();
			
			//Collect Partition
			if (MapReduce.UNITEST) {
				for (int i = 0; i < 2; i++) {
					String tmpName = "tmp/TaskTracker-001/Mapper/job001-task001-" + i;
					recordReconstructor.reconstruct(tmpName);
				}
			} else {
				rr.collectPartition(recordReconstructor);
			}
			
			System.out.println("RunReducer: Start to sort");
			
			//Merge Sort & construct Iterator
			recordReconstructor.sort();
			recordReconstructor.merge();
			
			Thread.sleep(1000 * 10); //TODO: remove this after debugging
			
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
			System.out.println("RemoteException caught");
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
			
			String localFileName = this.task.localReducerFileNameWrapper(taskEntry.getTID());
			File localFile = new File(localFileName);
			FileOutputStream fout = new FileOutputStream(localFile);
			
			Socket soc = new Socket(taskEntry.getHost(), taskEntry.getPort());
			PrintWriter out = new PrintWriter(soc.getOutputStream(), true);
			BufferedInputStream in = new BufferedInputStream(soc.getInputStream());
			
			String request = String.format("%s\n", this.task.remoteFileNameWrapper(this.task.getSEQ(), taskEntry.getTID()));
			System.out.println("REQUESTING:" + request + "\tTO:" + this.task.localReducerFileNameWrapper(taskEntry.getTID()));
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
			soc.close();
			
			Collector<K1, V1> collectorRunnable = new Collector<K1, V1>(recordReconstructor, localFileName);
			Thread collectorThread = new Thread(collectorRunnable);
			collectorThread.start();
//			collectorThread.join();
			threadList.add(collectorThread);
		}
		
		for (Thread th : threadList) {
			th.join();
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
			System.out.println("RunReducer.Collector.run(): Start to recontsruct file=" + this.fileName);
			try {
				this.rr.reconstruct(this.fileName);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				System.exit(12);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(13);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				System.exit(14);
			}
		}
		
	}
}
