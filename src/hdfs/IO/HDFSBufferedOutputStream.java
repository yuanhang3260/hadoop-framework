package hdfs.IO;

import global.Hdfs;

import java.io.IOException;
import java.io.Serializable;
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
		this.buff = new byte[Hdfs.Core.WRITE_BUFF_SIZE];
	}
	
	public void write(byte[] b) throws ArrayIndexOutOfBoundsException, IOException {
		this.write(b, 0, b.length);
	}
	
	public void write(byte[] b, int offset, int len) throws ArrayIndexOutOfBoundsException, IOException {
		
		if (Hdfs.Core.DEBUG && this.DEBUG) {
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
					
					if (Hdfs.Core.DEBUG && this.DEBUG) {
						System.out.print("Flush:" + new String(this.buff));
					}
					
					/* Reset the inner buffer offset */
					this.buffOffset = 0;
					if (Hdfs.Core.DEBUG && this.DEBUG) {
						System.out.println("\twritten=" + written + "\tpointer=" + (offset + written) + "\tbuffOffset=" + this.buffOffset);
					}
				}
			}
		}
		
		if (Hdfs.Core.DEBUG && this.DEBUG) {
			System.out.format("<------------------status:buff offset=%d\n\n\n",this.buffOffset);
		}
	}
	
	public void close() throws IOException {
		if (this.buffOffset != 0) {
			if (Hdfs.Core.DEBUG && this.DEBUG) {
				System.out.format("DEBUG HDFSBufferedOutputStream.close():\t[status:%d, buff:%s]\n"
						, this.buffOffset, new String(this.buff, 0, this.buffOffset));
			}
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
