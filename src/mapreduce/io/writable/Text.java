package mapreduce.io.writable;

public class Text extends Writable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9216926564051053141L;
	private String value;
	
	public Text(String text) {
		this.value = text;	//Initiate value
	}
	
	public String getText() {
		return this.value;
	}

	@Override
	public int getHashValue() {
		return this.value.hashCode();
	}
	
	public String toString() {
		return this.value;
	}
	
}
