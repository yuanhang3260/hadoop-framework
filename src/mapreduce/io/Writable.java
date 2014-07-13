package mapreduce.io;

import java.io.Serializable;

public abstract class Writable implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7468134504127619796L;
	public long hashValue;
	
	public int compareTo(Writable o) {
		if (this.hashValue > o.hashValue) {
			return 1;
		} else if (this.hashValue < o.hashValue) {
			return -1;
		} else {
			return 0;
		}
	}
}
