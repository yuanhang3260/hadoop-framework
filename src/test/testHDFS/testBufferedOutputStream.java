package test.testHDFS;

import global.Hdfs;
import hdfs.IO.HDFSBufferedOutputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class testBufferedOutputStream {
	
	public static void main(String[] args) throws IOException {
		File file1 = new File("./test_file/buff_0");
		File file2 = new File("./test_file/buff_1");
		
		if (!file1.exists()) {
			file1.createNewFile();
		}
		
		if (!file2.exists()) {
			file2.createNewFile();
		}
		
		String str1 = "abcdefghijklmn";
		String str2 = "1234567";
		byte[] buff1 = str1.getBytes();
		byte[] buff2 = str2.getBytes();
		
		FileOutputStream fout = new FileOutputStream(file1);
		HDFSBufferedOutputStream hout = new HDFSBufferedOutputStream(new FileOutputStream(file2));
		
		System.out.println("BUFFSIZE=" + Hdfs.WRITE_BUFF_SIZE);
		
		int len1 = 4;
		fout.write(buff1, 0, len1);
		hout.write(buff1, 0, len1);
		fout.write(buff1, len1, buff1.length - len1);
		hout.write(buff1, len1, buff1.length - len1);
		
		fout.write(buff2, 0, buff2.length);
		hout.write(buff2, 0, buff2.length);
		
		hout.close();
		fout.close();
	
	}
}
