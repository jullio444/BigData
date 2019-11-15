package hadoop.part5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CreditCardWritable implements WritableComparable<CreditCardWritable> {
	private Text supermarket, city;

	// default constructor for (de)serialization
	public CreditCardWritable() {
		supermarket = new Text();
		city = new Text();
	}

	public CreditCardWritable(Text supermarket,  Text city, IntWritable fraudOccurencies) {
		this.supermarket = supermarket;
		this.city = city;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		city.write(dataOutput);
		supermarket.write(dataOutput);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		city.readFields(dataInput);
		supermarket.readFields(dataInput);
	}


	public Text getSupermarket() {
		return supermarket;
	}

	public void setSupermarket(Text supermarket) {
		this.supermarket = supermarket;
	}
	public void setCity(Text city) {
		this.city = city;
	}

	public Text getCity() {
		return city;
	}
	@Override
	public int compareTo(CreditCardWritable o) {
		int k = this.getSupermarket().compareTo(o.getSupermarket());
		k = k == 0 ? this.getCity().compareTo(o.getCity()) : k;

			return k;
	}



	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null || getClass() != obj.getClass())
			return false;
		CreditCardWritable ex = (CreditCardWritable) obj;
		return Objects.equals(supermarket, ex.supermarket) && Objects.equals(city, ex.city);
	}

	@Override
	public int hashCode() {
		return Objects.hash(supermarket, city);
	}
	@Override
	public String toString() {
		return "( "+supermarket+" , "+city +" )";
	}
}