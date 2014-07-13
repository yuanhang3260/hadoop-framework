package mapreduce.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;

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
		File taskFile = new File(args[0]);
		try {
			FileInputStream fin = new FileInputStream(taskFile);
			ObjectInputStream in = new ObjectInputStream(fin);
			rm.task = (Task)in.readObject();
			
			OutputCollector<Writable, Writable> output = new OutputCollector<Writable, Writable>(rm.task.partitionNum);
			RecordReader recordReader = new RecordReader();
			rm.mapper = (Mapper<Writable, Writable, Writable, Writable>) rm.task.taskClass.getConstructors()[0].newInstance();
			while (recordReader.hasNext()) {
				rm.mapper.map(recordReader.nextKey(), recordReader.nextValue(), output);
			}
			output.writeToLocal();
			in.close();
			fin.close();
			return;
			
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
}
