package hdfs.IO;

import global.Hdfs;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

public class HDFSBufferedOutputStream implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4252529741647442999L;
	private int buffOffset;
	private byte[] buff;
	private HDFSOutputStream outputStream;
	
	private boolean toHDFS;
	private FileOutputStream outputStreamToLocal;
	
	public HDFSBufferedOutputStream (HDFSOutputStream outputStream) {
		this.toHDFS  = true;
		this.outputStream = outputStream;
		this.buffOffset = 0;
		this.buff = new byte[Hdfs.WRITE_BUFF_SIZE];
	}
	
	public HDFSBufferedOutputStream (FileOutputStream outputStream) {
		this.toHDFS = false;
		this.outputStreamToLocal = outputStream;
		this.buffOffset = 0;
		this.buff = new byte[Hdfs.WRITE_BUFF_SIZE];
	}
	
	public void write(byte[] b, int offset, int len) throws IOException {
		
		System.out.println("------------------>Objective:" + new String(b, offset, len));
		
		if (offset + len > b.length) {
			throw new IOException("Out of index");
		}
		
		if ((this.buff.length - this.buffOffset) >= len) {
			System.arraycopy(b, offset, this.buff, this.buffOffset, len);
			this.buffOffset += len;
			System.out.println("Finish writing, current buffer:" + new String(this.buff, 0, this.buffOffset));
		} 
		
		else {
			
			int written = 0;
			
			while (written < len) {
				
				int towrite = Math.min(len - written, this.buff.length - this.buffOffset);
				
				System.out.format("Put %d to buffer.\n", towrite);
				
				System.arraycopy(b, offset + written, this.buff, this.buffOffset, towrite);
				this.buffOffset += towrite;
				written += towrite;
				
				if (written == len) {
					break;
				} else {
					if (toHDFS) {
						this.outputStream.write(buff);
					} else {
						this.outputStreamToLocal.write(buff);
						System.out.print("Flush:" + new String(this.buff));
					}
					this.buffOffset = 0;
					System.out.println("\twritten=" + written + "\tpointer=" + (offset + written) + "\tbuffOffset=" + this.buffOffset);
				}
				
				
			}
		}
		System.out.format("<------------------status:buff offset=%d\n\n\n",this.buffOffset);
	}
	
	public void close() throws IOException {
		if (toHDFS) {
			if (this.buffOffset != 0) {
				this.outputStream.write(Arrays.copyOfRange(this.buff, 0, this.buffOffset));
			}
			this.outputStream.close();
		} else {
			if (this.buffOffset != 0) {
				this.outputStreamToLocal.write(Arrays.copyOfRange(this.buff, 0, this.buffOffset));
				System.out.println("Close:" + new String(this.buff, 0, this.buffOffset));
			}
			this.outputStreamToLocal.close();
		}
	}
}
