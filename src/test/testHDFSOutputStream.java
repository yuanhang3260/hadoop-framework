package test;

import java.io.IOException;

import hdfs.IO.HDFSOutputStream;

public class testHDFSOutputStream {
	public static void main(String[] args) throws IOException {
		HDFSOutputStream out = new HDFSOutputStream(7);
		int buff1_len = 7;
		int buff2_len = 0;
		byte[] buff1 = new byte[buff1_len];
		for (int i = 0; i < buff1_len; i++) {
			buff1[i] = (byte)('1' + i);
		}
		byte[] buff2 = new byte[buff2_len];
		for (int i = 0; i < buff2_len; i++) {
			buff2[i] = (byte) ('a' + i);
		}
		out.write(buff1);
		out.write(buff2);
	}
}
