package mapreduce.task;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;

public class TaskTracker implements TaskTrackerRemoteInterface {
	String name;
	
	@Override
	public boolean runTask(Task task) throws RemoteException {
		File taskFile = new File(String.format("tmp/%s-%s-taskFile", name, task.tid));
		try {
			taskFile.createNewFile();
			FileOutputStream fw = new FileOutputStream(taskFile);
			ObjectOutputStream out = new ObjectOutputStream(fw);
			out.writeObject(task);
			out.close();
		} catch (IOException e) {
			return false;
		}
		
		return true;
	}

	@Override
	public byte[] transferPartition(String fileName) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
		
	}
	
	
}
