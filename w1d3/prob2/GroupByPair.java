package bigData.w1d3.prob2;

public class GroupByPair<K,V> {
	private K k;
	private V v;
	
	GroupByPair(K k, V v){
		this.k = k;
		this.v = v;
	}
	public K getK() {
		return k;
	}
	public V getV(){
		return v;
	}
	@Override
	public String toString() {
		return "< "+k+" , "+v+" >";
	}
}
