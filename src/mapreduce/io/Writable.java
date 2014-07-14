package mapreduce.io;

import java.io.Serializable;

public abstract class Writable implements Serializable {

	private static final long serialVersionUID = -7468134504127619796L;
	
	public final int MAX_KEY = Integer.MAX_VALUE;
	public final int MIN_KEY = Integer.MIN_VALUE;
	
	public abstract int getHashValue();
	
	public int compareTo(Writable o) {
		if (this.getHashValue() > o.getHashValue()) {
			return 1;
		} else if (this.getHashValue() < o.getHashValue()) {
			return -1;
		} else {
			return 0;
		}
	}
}
