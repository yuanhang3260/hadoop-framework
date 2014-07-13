package mapreduce.core;

public abstract class Mapper<K1, V1, K2, V2> {
	
	public abstract void map(K1 key, V1 value, OutputCollector<K2, V2> output);
}
