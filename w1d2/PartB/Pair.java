package bigData.w1d2.PartB;

public class Pair<K, V> {
	private K k;
	private V v;

	Pair(K k, V v) {
		this.k = k;
		this.v = v;
	}

	public K getK() {
		return k;
	}

	public void setK(K k) {
		this.k = k;
	}

	public V getV() {
		return v;
	}

	@Override
	public String toString() {
		return "< " + k + ", " + v + " >\n";
	}
}