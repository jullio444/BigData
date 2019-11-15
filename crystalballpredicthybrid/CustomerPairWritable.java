package hadoop.crystalballpredicthybrid;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomerPairWritable implements WritableComparable<CustomerPairWritable> {
	private Text itemA;
	private Text itemB;

	// default constructor for (de)serialization
	public CustomerPairWritable() {
		itemA = new Text();
		itemB = new Text();
	}

	public CustomerPairWritable(Text itemA, Text itemB) {
		this.itemA = itemA;
		this.itemB = itemB;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		itemA.write(dataOutput);
		itemB.write(dataOutput);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		itemA.readFields(dataInput);
		itemB.readFields(dataInput);
	}

	public Text getItemA() {
		return itemA;
	}

	public void setItemA(Text itemA) {
		this.itemA = itemA;
	}

	public Text getItemB() {
		return itemB;
	}

	public void setItemB(Text itemB) {
		this.itemB = itemB;
	}

	@Override
	public int compareTo(CustomerPairWritable obj) {
		int cmp = this.itemA.compareTo(obj.itemA);
		if (cmp != 0)
			return cmp;
		return this.itemB.compareTo(obj.itemB);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null || getClass() != obj.getClass())
			return false;
		CustomerPairWritable cwObj = (CustomerPairWritable) obj;
		return cwObj.itemA.equals(this.itemA) && cwObj.itemB.equals(this.itemB);
	}

	@Override
	public int hashCode() {
		return Objects.hash(itemA, itemB);
	}

	@Override
	public String toString() {
		return "( " + itemA + " , " + itemB + " )";
	}
}