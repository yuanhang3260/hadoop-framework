package mapreduce.io.writable;

import java.io.Serializable;

public abstract class Writable implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7468134504127619796L;
	
	public final int MAX_KEY = Integer.MAX_VALUE;
	public final int MIN_KEY = Integer.MIN_VALUE;
	
	public abstract int getHashValue();
	public abstract String toString();


}
