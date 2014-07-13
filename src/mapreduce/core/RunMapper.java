package mapreduce.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;

public class RunMapper {
	public Task task;
	public Mapper mapper;
	
	public static void main(String[] args) {
		RunMapper rm = new RunMapper();
		File taskFile = new File(args[0]);
		try {
			FileInputStream fin = new FileInputStream(taskFile);
			ObjectInputStream in = new ObjectInputStream(fin);
			rm.task = (Task)in.readObject();
			
			OutputCollector output = new OutputCollector<Writable, Writable>(rm.task.partitionNum);
			RecordReader recordReader = new RecordReader();
			rm.mapper = (Mapper) rm.task.taskClass.getConstructors()[0].newInstance();
			while (recordReader.hasNext()) {
				rm.mapper.map(recordReader.nextKey(), recordReader.nextValue(), output);
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
