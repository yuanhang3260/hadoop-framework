package mapreduce.core;

import global.MapReduce;
import global.Parser;
import global.Parser.ConfOpt;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import mapreduce.io.KeyValue;
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
		
		try {
			Parser.hdfsCoreConf();
			Parser.printConf(new ConfOpt[] {ConfOpt.HDFSCORE});
		} catch (Exception e) {
			e.printStackTrace();
			
			System.err.println("The Mapper task cannot read configuration info.\n"
					+ "Please confirm the hdfs.xml is placed as ./conf/hdfs.xml.\n"
					+ "The  Mapper task cannot is shutting down...");
			
			System.exit(1);
		}
		
		RunMapper<Writable, Writable, Writable, Writable> rm = new RunMapper<Writable, Writable, Writable, Writable>();
		
		try {
			
			/*------------------ Retrieve Task ----------------*/
			if (MapReduce.Core.DEBUG) {
				System.out.println("DEBUG RunMapper.main(): Try to Retrived task.");
			}
			
			Registry taskTrackerR = LocateRegistry.getRegistry("localhost", Integer.parseInt(args[0]));
			TaskTrackerRemoteInterface taskTrackerS = (TaskTrackerRemoteInterface) taskTrackerR
					.lookup(MapReduce.TaskTracker.Common.TASK_TRACKER_SERVICE_NAME);
			rm.task = (MapperTask) taskTrackerS.getTask(args[1]);
			
			
			
			/* The following block is for fault-tolerance test */
			if (MapReduce.TaskTracker.Common.MAPPER_FAULT_TEST) {
				try {
					Thread.sleep(1000 * 2);
				} catch (InterruptedException e) { }
				System.exit(128);
			}
			
			/*------------------ Prepare input records ----------------*/
			
			if (MapReduce.Core.DEBUG) {
				System.out.println("DEBUG RunMapper.main(): "
						+ "RunMapper retrived task and start to prepare records");
			}
			KeyValueLineRecordReader recordReader = new KeyValueLineRecordReader(rm.task.split);
			recordReader.parseRecords();
			
			
			/*------------------ Map Phase ----------------*/
			
			if (MapReduce.Core.DEBUG) {
				System.out.println("DEBUG RunMapper.main(): "
						+ "All input records are ready and now move to map phase.");
			}
			
			//STEP 1: Load mapper class
			
			Class<Mapper<Writable, Writable, Writable, Writable>> mapperClass = 
					rm.loadClass();
			
			//STEP 2: Run mapper

			OutputCollector<Writable, Writable> output = new OutputCollector<Writable, Writable>(rm.task);
			
			rm.mapper = (Mapper<Writable, Writable, Writable, Writable>) mapperClass.getConstructors()[0].newInstance();
			
			while (recordReader.hasNext()) {
				KeyValue<Text, Text> nextLine = recordReader.nextKeyValue();
				rm.mapper.map(nextLine.getKey(), nextLine.getValue(), output);
			}
			
			/*------------------ Sort intermediate values ----------------*/
			
			if (MapReduce.Core.DEBUG) {
				System.out.println("DEBUG RunMapper.main(): Finish map phase and start to sort intermediate <key, value> pair by key.");
			}
			output.sort();
			
			/*------------------ Write intermediate values to local  ----------------*/
			
			if (MapReduce.Core.DEBUG) {
				System.out.println("DEBUG RunMapper.main(): The last step is to write mapper intermediate value to local file system.");
			}
			output.writeToLocal();
			
			return;
		} catch (NumberFormatException e) {
			if (MapReduce.Core.DEBUG) {e.printStackTrace();}
			System.exit(1);
		} catch (RemoteException e) {
			if (MapReduce.Core.DEBUG) {e.printStackTrace();}
			System.exit(2);
		} catch (NotBoundException e) {
			if (MapReduce.Core.DEBUG) {e.printStackTrace();}
			System.exit(3);
		} catch (IOException e) {
			if (MapReduce.Core.DEBUG) {e.printStackTrace();}
			System.exit(4);
		} catch (IllegalArgumentException e) {
			if (MapReduce.Core.DEBUG) {e.printStackTrace();}
			System.exit(5);
		} catch (SecurityException e) {
			if (MapReduce.Core.DEBUG) {e.printStackTrace();}
			System.exit(6);
		} catch (InstantiationException e) {
			if (MapReduce.Core.DEBUG) {e.printStackTrace();}
			System.exit(7);
		} catch (IllegalAccessException e) {
			if (MapReduce.Core.DEBUG) {e.printStackTrace();}
			System.exit(8);
		} catch (InvocationTargetException e) {
			if (MapReduce.Core.DEBUG) {e.printStackTrace();}
			System.exit(9);
		} catch (ClassNotFoundException e) {
			if (MapReduce.Core.DEBUG) {e.printStackTrace();}
			System.exit(10);
		}
		
	}	
	
	public Class<Mapper<Writable, Writable, Writable, Writable>> loadClass ()
			throws IOException, ClassNotFoundException {
		
		/* Load Jar file */
		String jarFilePath = this.task.getJarLocalPath();
		JarFile jarFile = new JarFile(jarFilePath);
		Enumeration<JarEntry> e = jarFile.entries();
		
		URL[] urls = { new URL("jar:file:" + jarFilePath +"!/") };
		ClassLoader cl = URLClassLoader.newInstance(urls);
		
		Class<Mapper<Writable, Writable, Writable, Writable>> mapperClass = null;
		
		/* Iterate .class files */
		while (e.hasMoreElements()) {
            
			JarEntry je = e.nextElement();
            
			if(je.isDirectory() || !je.getName().endsWith(".class")){
                continue;
            }
            
            String className = je.getName().substring(0, je.getName().length() - 6);
            className = className.replace('/', '.');
            if (className.equals(this.task.getMapperClassName())) {
            	mapperClass = (Class<Mapper<Writable, Writable, Writable, Writable>>) cl.loadClass(className);
            }
        }
		
		return mapperClass;
	}
}
