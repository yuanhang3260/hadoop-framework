package hdfs.IO;

import global.Hdfs;
import hdfs.DataStructure.HDFSFile;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;

public class HDFSBufferedOutputStream implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4252529741647442999L;
	private boolean DEBUG = false;
	private int buffOffset;
	private byte[] buff;
	private HDFSOutputStream outputStream;
	
	public HDFSBufferedOutputStream (HDFSOutputStream outputStream) {
		this.outputStream = outputStream;
		this.buffOffset = 0;
		this.buff = new byte[Hdfs.Common.WRITE_BUFF_SIZE];
	}
	
	public static void main (String[] args) throws NotBoundException, IOException {
		File file = new File("./test_file/hello");
		byte[] buff = new byte[128];
		FileInputStream fin = new FileInputStream(file);
		
		Registry nameNodeR = LocateRegistry.getRegistry(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort);
		NameNodeRemoteInterface nameNodeI = (NameNodeRemoteInterface) nameNodeR.lookup(Hdfs.Common.NAME_NODE_SERVICE_NAME);
		HDFSFile fileNONBuffered = nameNodeI.create("non-buffered");
		HDFSFile fileBuffered = nameNodeI.create("buffered");
		
		HDFSOutputStream hout = fileNONBuffered.getOutputStream();
		HDFSBufferedOutputStream hbout = new HDFSBufferedOutputStream(fileBuffered.getOutputStream());
		
		int c = 0;
		while ((c = fin.read(buff)) != -1) {
			if (Hdfs.Common.DEBUG) {
				System.out.format("Write %d bytes\n", c);
			}
			if (c == 128) {
				hout.write(buff);
			} else {
				byte[] tmp_buff = Arrays.copyOfRange(buff, 0, c);
				hout.write(tmp_buff);
			}
			hbout.write(buff, 0, c);
		}
		
		hout.close();
		hbout.close();
		fin.close();
	}
	
	public void write(byte[] b) throws ArrayIndexOutOfBoundsException, IOException {
		this.write(b, 0, b.length);
	}
	
	public void write(byte[] b, int offset, int len) throws ArrayIndexOutOfBoundsException, IOException {
		
		if (Hdfs.Common.DEBUG && this.DEBUG) {
			System.out.println("------------------>Objective:" + new String(b, offset, len));
		}
		
		if (offset + len > b.length) {
			throw new ArrayIndexOutOfBoundsException("" + offset + len);
		}
		
		if ((this.buff.length - this.buffOffset) >= len) {
			System.arraycopy(b, offset, this.buff, this.buffOffset, len);
			this.buffOffset += len;
		} 
		
		else {
			
			int written = 0;
			
			while (written < len) {
				
				int towrite = Math.min(len - written, this.buff.length - this.buffOffset);
				
				/* Copy to buffer */
				System.arraycopy(b, offset + written, this.buff, this.buffOffset, towrite);
				this.buffOffset += towrite;
				written += towrite;
				
				/* Stop writing to buffer if all bytes are fit in */
				if (written == len) {
					break;
				} else {
					this.outputStream.write(buff);
					
					if (Hdfs.Common.DEBUG && this.DEBUG) {
						System.out.print("Flush:" + new String(this.buff));
					}
					
					/* Reset the inner buffer offset */
					this.buffOffset = 0;
					if (Hdfs.Common.DEBUG && this.DEBUG) {
						System.out.println("\twritten=" + written + "\tpointer=" + (offset + written) + "\tbuffOffset=" + this.buffOffset);
					}
				}
			}
		}
		
		if (Hdfs.Common.DEBUG && this.DEBUG) {
			System.out.format("<------------------status:buff offset=%d\n\n\n",this.buffOffset);
		}
	}
	
	public void close() throws IOException {
		if (this.buffOffset != 0) {
			System.out.format("DEBUG HDFSOutputStream.close():\t[status:%d, buff:%s]\n", this.buffOffset, new String(this.buff, 0, this.buffOffset));
			this.outputStream.write(Arrays.copyOfRange(this.buff, 0, this.buffOffset));
		}
		this.outputStream.close();
	}
	
	public void flush() throws IOException {
		if (this.buffOffset != 0) {
			this.outputStream.write(Arrays.copyOfRange(this.buff, 0, this.buffOffset));
		}
	}
}
