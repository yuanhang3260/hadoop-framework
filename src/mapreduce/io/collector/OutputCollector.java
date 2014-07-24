package mapreduce.io.collector;

import global.Hdfs;
import global.MapReduce;
import hdfs.io.HDFSBufferedOutputStream;
import hdfs.io.HDFSFile;
import hdfs.io.HDFSOutputStream;
import hdfs.namenode.NameNodeRemoteInterface;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.PriorityQueue;
import java.util.Queue;

import mapreduce.io.KeyValue;
import mapreduce.io.Partitioner;
import mapreduce.io.writable.IntWritable;
import mapreduce.io.writable.Text;
import mapreduce.io.writable.Writable;
import mapreduce.message.MapperTask;
import mapreduce.message.Task;

/**
 * The OutputCollector is used for collecting the output
 * results of the Mapper and Reducer. 
 * 
 * @author JeremyFu and KimWei
 *
 * @param <K>
 * @param <V>
 */
public class OutputCollector<K extends Writable, V extends Writable> {
	
	private Queue<KeyValue<K, V>> keyvalueQueue; 
	
	private boolean forMapper;
	
	File[] fileArr;
	
	FileOutputStream[] fileOutputStreamArr;
	
	BufferedOutputStream[] bufferedOutputStreamArr;
	
	Task task;
	
	int partitionNum;
	
	private int fileCounter = 0;
	private final int FILE_MAX_LINES = MapReduce.Core.FILE_MAX_LINES;
	private HDFSBufferedOutputStream bout;
	private String reducerTmpFile;
	
	
	
	/**
	 * OutputCollector constructor: This is for mappers, which
	 * write the outputs to different partition files on local
	 * file system.
	 * @param task A maper task
	 */
	public OutputCollector(MapperTask task) {
		this.task = task;
		this.partitionNum = ((MapperTask)this.task).getPartitionNum();
		
		this.keyvalueQueue = new PriorityQueue<KeyValue<K, V>>();
		this.bufferedOutputStreamArr = new BufferedOutputStream[this.partitionNum];
		this.forMapper = true;
	}
	
	/**
	 * OutputCollector constructor: This is for reducers, which
	 * merges same partitions collected from different mappers
	 * to a file on HDFS.
	 * @throws NotBoundException 
	 * @throws IOException 
	 */
	public OutputCollector(String filename, String tmpFile) throws NotBoundException, IOException {
		this.keyvalueQueue = new PriorityQueue<KeyValue<K, V>>();
		this.bufferedOutputStreamArr = new BufferedOutputStream[1];
		this.forMapper = false;
		
		Registry nameNodeR = LocateRegistry.getRegistry(Hdfs.Core.NAME_NODE_IP, Hdfs.Core.NAME_NODE_REGISTRY_PORT);
		NameNodeRemoteInterface nameNodeS = (NameNodeRemoteInterface) nameNodeR.lookup(Hdfs.Core.NAME_NODE_SERVICE_NAME);
		HDFSFile file = nameNodeS.create(filename);
		HDFSOutputStream out = file.getOutputStream();
		this.bout = new HDFSBufferedOutputStream(out);
		this.reducerTmpFile = tmpFile;
		
	}
	
	/**
	 * This method collect the output of Mappers and Reducers.
	 * In order to prevent running out of memory, the output
	 * collector flush the key value pair to files. And an 
	 * external sorting is used later if needed.
	 * 
	 * @param key The output key.
	 * @param value The output value
	 * @throws IOException 
	 */
	public void collect(K key, V value) throws IOException {
		
		this.keyvalueQueue.offer(new KeyValue<K, V>(key, value));	

		if ((this.keyvalueQueue.size()) % this.FILE_MAX_LINES == 0) { //write to temp file
			if (this.forMapper) {
				flushToLocal();
			} else {
				flushToHDFS(false);
			}
		}
 	}
	
	
	/**
	 * Flush the buffer of OutputCollector to local files.
	 * @throws IOException
	 */
	public void flushToLocal() throws IOException {
		String filePrefix = task.getFilePrefix();
		String filename = String.format("%s/%s-%s-tmp-%d.tmp", 
				filePrefix, this.task.getJobId(), this.task.getTaskId(), this.fileCounter++);
		
		File tmpFile = new File(filename);
		BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream(tmpFile));
		
		while (!this.keyvalueQueue.isEmpty()) {
			KeyValue<K, V> pair = this.keyvalueQueue.poll();
			String pairString = String.format("%s\t%s\n", 
					pair.getKey().toString(), pair.getValue().toString());
			bout.write(pairString.getBytes());
		}
		bout.flush();
		bout.close();
		this.keyvalueQueue = new PriorityQueue<KeyValue<K, V>>();
	}
	
	
	/**
	 * Flush the buffer of OutputCollector to HDFS.
	 * 
	 * @param close if 
	 * @throws IOException
	 */
	public void flushToHDFS(boolean close) throws IOException {
		while (!this.keyvalueQueue.isEmpty()) {
			KeyValue<K, V> pair = this.keyvalueQueue.poll();
			byte[] content = String.format("%s\t%s\n", 
					pair.getKey().toString(), pair.getValue().toString()).getBytes();
			this.bout.write(content);
		}
		
		if (close) {
			this.bout.close();
			File tmpFile = new File(this.reducerTmpFile);
			
			tmpFile.delete();
		}
	}
	
	/**
	 * This sort() method performs an external sort. The size of
	 * output <key, value> pair is unpredictable. External sort 
	 * can prevent memory from running out.
	 * @throws IOException
	 */
	public void sort() throws IOException {
		
		Partitioner<Text> partitioner = new Partitioner<Text>();
		
		BufferedReader[] inputArray = new BufferedReader[this.fileCounter];
		BufferedOutputStream[] outputArray = new BufferedOutputStream[this.partitionNum];
		
		/* Initialization */
		for (int i = 0; i < this.fileCounter; i++) {
			String filename = String.format("%s/%s-%s-tmp-%d.tmp",
					task.getFilePrefix(), task.getJobId(), task.getTaskId(), i);
			inputArray[i] = new BufferedReader(new FileReader(filename));
		}
		
		for (int i = 0; i < this.partitionNum; i++) {
			String filename = String.format("%s/%s-%s-%d",
					task.getFilePrefix(), task.getJobId(), task.getTaskId(), i);
			outputArray[i] = new BufferedOutputStream(new FileOutputStream(filename));
		}
		
		Queue<FileKeyValue> heap = new PriorityQueue<FileKeyValue>();
		
		/* External sort */
		for (int i = 0; i < this.fileCounter; i++) {
			String line = inputArray[i].readLine();
			String[] comp = line.split("\t");
			heap.offer(new FileKeyValue(i, new Text(comp[0]), new IntWritable(comp[1])));
		}
		
		while (!heap.isEmpty()) {
			
			FileKeyValue towrite = heap.poll();
			
			int parNum = partitioner.getPartition(towrite.getPair().getKey(), this.partitionNum);
			
			String buff = String.format("%s\t%s\n", towrite.getPair().getKey().toString(), towrite.getPair().getValue().toString());
			
			outputArray[parNum].write(buff.getBytes());
			
			String line = inputArray[towrite.getFileSEQ()].readLine();
			if (line != null) {
				String[] comp = line.split("\t");
				heap.offer(new FileKeyValue(towrite.getFileSEQ(), new Text(comp[0]), new IntWritable(comp[1])));
			}
		}
		
		/* Close */
		for (int i = 0; i < this.fileCounter; i++) {
			inputArray[i].close();;
		}
		
		for (int i = 0; i < this.partitionNum; i++) {
			outputArray[i].flush();
			outputArray[i].close();
		}
		
		/* delete temp file */
		for (int i = 0; i < this.fileCounter; i++) {
			String filename = String.format("%s/%s-%s-tmp-%d.tmp",
					task.getFilePrefix(), task.getJobId(), task.getTaskId(), i);
			File tmpFile = new File(filename);
			tmpFile.delete();
		}
		
	}
	
	
	/**
	 * This private nested class is used for tracking the relationship
	 * between file and KeyValue pair.
	 * 
	 * 
	 * @author JeremyFu and Kim Wei
	 *
	 */
	private class FileKeyValue implements Comparable<FileKeyValue> {
		
		private int fileSEQ;
		private KeyValue<Text, IntWritable> pair;
		
		public FileKeyValue (int seq, Text key, IntWritable value) {
			this.fileSEQ = seq;
			this.pair = new KeyValue<Text, IntWritable>(key, value);
		}
		
		public int getFileSEQ() {
			return this.fileSEQ;
		}
		
		public KeyValue<Text, IntWritable> getPair() {
			return this.pair;
		}

		@Override
		public int compareTo(FileKeyValue obj) {
			
			return this.getPair().compareTo(obj.getPair());
			
		}
		
	}
	
}
