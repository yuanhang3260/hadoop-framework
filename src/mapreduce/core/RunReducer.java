package mapreduce.core;

import global.MapReduce;
import global.Parser;
import global.Parser.ConfOpt;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.Inet4Address;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import mapreduce.io.KeyValueCollection;
import mapreduce.io.collector.OutputCollector;
import mapreduce.io.recordreader.TextIntReconstructor;
import mapreduce.io.writable.IntWritable;
import mapreduce.io.writable.Text;
import mapreduce.io.writable.Writable;
import mapreduce.message.PartitionEntry;
import mapreduce.message.ReducerTask;
import mapreduce.tasktracker.TaskTrackerRemoteInterface;

public class RunReducer <K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable> {
	
	public ReducerTask task;
	public Reducer<K1, V1, K2, V2> reducer;
	private static final int BUFF_SIZE = 1024;
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		
		RunReducer<Writable, Writable, Writable, Writable> rr = new RunReducer<Writable, Writable, Writable, Writable>();
		boolean toFail = false;
		
		try {
			Parser.hdfsCoreConf();
			if (MapReduce.Core.DEBUG) {
				Parser.printConf(new ConfOpt[] {ConfOpt.HDFSCORE});
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			
			System.err.println("The Reducer task cannot read configuration info.\n"
					+ "Please confirm the hdfs.xml is placed as ./conf/hdfs.xml.\n"
					+ "The  Reducer task cannot is shutting down...");
			
			System.exit(1);
		}
		
		try {
			
			/*----- Retrieve task --------*/
			Registry taskTrackerR = LocateRegistry.getRegistry("localhost", Integer.parseInt(args[0]));
			TaskTrackerRemoteInterface taskTrackerS = (TaskTrackerRemoteInterface) taskTrackerR
					.lookup(MapReduce.TaskTracker.Common.TASK_TRACKER_SERVICE_NAME);
			rr.task = (ReducerTask) taskTrackerS.getTask(args[1], args[2]);


			
			TextIntReconstructor recordReconstructor = 
					new TextIntReconstructor ();
			
			
			if (MapReduce.TaskTracker.Common.REDUCER_FAULT_TEST && toFail) {
				System.exit(134);
			}
			
			/*------ Collect partitions -----*/
			rr.collectPartition();

			
			/*------ Sort -----*/
			if (MapReduce.Core.DEBUG) {
				System.out.println("DEBUG RunReducer.main(): Start to sort");
			}
			recordReconstructor.sort(rr.task);
			
			
			/*---------- Reduce ------------*/
			if (MapReduce.Core.DEBUG) {
				System.out.println("DEBUG RunReducer.main(): Finish merging and start to reduce");
			}
			
			//STEP 1: Load reducer class
			
			Class<Reducer<Writable, Writable, Writable, Writable>> reducerClass = 
					rr.loadClass();
			
			//STEP 2: Run mapper
			OutputCollector<Writable, Writable> output = new OutputCollector<Writable, Writable>(rr.task.getOutputPath(), recordReconstructor.getReducerTempFile());
			
			rr.reducer = (Reducer<Writable, Writable, Writable, Writable>) reducerClass.getConstructors()[0].newInstance();
			
			while (recordReconstructor.hasNext()) {
				KeyValueCollection<Text, IntWritable> nextLine = recordReconstructor.nextKeyValueCollection();
				rr.reducer.reduce(nextLine.getKey(), nextLine.getValues().iterator(), output);
			}
			
//			/*----------- Sort Output by key -----------*/
//			if (MapReduce.Core.DEBUG) {
//				System.out.println("DEBUG RunReducer.main(): Finish reducing and start to sort output");
//			}
//			output.sort(); //TODO: check the necessity of sort again
			
			/*------------ Write to HDFS ---------------*/
			if (MapReduce.Core.DEBUG) {
				System.out.println("DEBUG RunReducer.main(): Finish sorting and start write to HDFS");
			}
			output.flushToHDFS(true);;
			
		} catch (RemoteException e) {
			if (MapReduce.Core.DEBUG) { e.printStackTrace(); }
			System.exit(1);
		} catch (FileNotFoundException e) {
			if (MapReduce.Core.DEBUG) { e.printStackTrace(); }
			System.exit(2);
		} catch (IOException e) {
			if (MapReduce.Core.DEBUG) { e.printStackTrace(); }
			System.exit(3);
		} catch (InterruptedException e) {
			if (MapReduce.Core.DEBUG) { e.printStackTrace(); }
			System.exit(5);
		} catch (IllegalArgumentException e) {
			if (MapReduce.Core.DEBUG) { e.printStackTrace(); }
			System.exit(6);
		} catch (SecurityException e) {
			if (MapReduce.Core.DEBUG) { e.printStackTrace(); }
			System.exit(7);
		} catch (InstantiationException e) {
			if (MapReduce.Core.DEBUG) { e.printStackTrace(); }
			System.exit(8);
		} catch (IllegalAccessException e) {
			if (MapReduce.Core.DEBUG) { e.printStackTrace(); }
			System.exit(9);
		} catch (InvocationTargetException e) {
			if (MapReduce.Core.DEBUG) { e.printStackTrace(); }
			System.exit(10);
		} catch (NotBoundException e) {
			if (MapReduce.Core.DEBUG) { e.printStackTrace(); }
			System.exit(11);
		} catch (ClassNotFoundException e) {
			if (MapReduce.Core.DEBUG) { e.printStackTrace(); }
			e.printStackTrace();
			System.exit(12);
		}
		
	}
	
	public void collectPartition() 
			throws UnknownHostException, IOException, InterruptedException, ClassNotFoundException {

		for (PartitionEntry taskEntry : this.task.getEntries()) {
			
			String localFileName = this.task.localReducerFileNameWrapper(taskEntry.getTID());
			File localFile = new File(localFileName);
			
			if (taskEntry.getHost().equals((Inet4Address.getLocalHost().getHostAddress()))) { 
				
				if (MapReduce.Core.DEBUG) {
					System.out.println("RunReducer.collectPartition(): Get file from local file system");
				}
				
				String srcFileName = this.task.remoteFileNameWrapper(this.task.getSEQ(), taskEntry.getTID());
				
				File srcFile = new File(this.task.getLocalMapperFilePrefix() + "/" + srcFileName);  //Mapper file
				
				if (!srcFile.exists()) {
					throw new IOException("Cannot found mapper file on local");
				} else {
					srcFile.renameTo(localFile);
				}
				
			} else {
				
				if (MapReduce.Core.DEBUG) {
					System.out.println("RunReducer.collectPartition(): Get file from other TaskTracker server.");
				}
				
				FileOutputStream fout = new FileOutputStream(localFile);
			
				Socket soc = new Socket(taskEntry.getHost(), taskEntry.getPort());
				PrintWriter out = new PrintWriter(soc.getOutputStream(), true);
				BufferedInputStream in = new BufferedInputStream(soc.getInputStream());
				
				String request = String.format("mapper-file\n%s\n", this.task.remoteFileNameWrapper(this.task.getSEQ(), taskEntry.getTID()));
				//System.out.println("REQUESTING:" + request + "\tIP=" + taskEntry.getHost() + "\tTO:" + this.task.localReducerFileNameWrapper(taskEntry.getTID()));
				out.write(request.toCharArray());
				out.flush();
			
				byte[] buff = new byte[BUFF_SIZE];
				int readBytes = 0;
				
				while ((readBytes = in.read(buff)) != -1) {
					fout.write(buff, 0, readBytes);
				}
				
				in.close();
				out.close();
				fout.close();
				soc.close();
			}
		}

	}
	
	@SuppressWarnings("unchecked")
	public Class<Reducer<Writable, Writable, Writable, Writable>> loadClass ()
			throws IOException, ClassNotFoundException {
		
		/* Load Jar file */
		String jarFilePath = this.task.getJarEntry().getLocalPath();
		if (MapReduce.Core.DEBUG) {
			System.out.println("DEBUG RunReducer.loadClass(): JarFilePath:" + jarFilePath);
		}
		JarFile jarFile = new JarFile(jarFilePath);
		Enumeration<JarEntry> e = jarFile.entries();
		
		URL[] urls = { new URL("jar:file:" + jarFilePath +"!/") };
		ClassLoader cl = URLClassLoader.newInstance(urls);
		
		Class<Reducer<Writable, Writable, Writable, Writable>> reducerClass = null;
		
		/* Iterate .class files */
		while (e.hasMoreElements()) {
            
			JarEntry je = e.nextElement();
            
			if(je.isDirectory() || !je.getName().endsWith(".class")){
                continue;
            }
            
            String className = je.getName().substring(0, je.getName().length() - 6);
            className = className.replace('/', '.');
            if (className.equals(this.task.getReducerClassName())) {
            	reducerClass = (Class<Reducer<Writable, Writable, Writable, Writable>>) cl.loadClass(className);
            }
        }
		
		return reducerClass;
		
	}
}
