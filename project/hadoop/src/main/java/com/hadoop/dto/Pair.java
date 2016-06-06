package com.hadoop.dto;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
	private String key;
	private String value;

	public Pair() {
		this.key = "";
		this.value = "";
	}

	public Pair(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public void readFields(DataInput arg) throws IOException {
		if (arg == null) {
			throw new IllegalArgumentException("Input cannot be NULL.");
		}
		this.key = arg.readUTF();
		this.value = arg.readUTF();
	}

	@Override
	public void write(DataOutput arg) throws IOException {
		if (arg == null) {
			throw new IllegalArgumentException("Output cannot be NULL.");
		}
		arg.writeUTF(this.key);
		arg.writeUTF(this.value);
	}

	@Override
	public int compareTo(Pair o) {
		if (o == null || !(o instanceof Pair))
			return -1;
		int cmp = this.key.compareTo(o.key);
		if (cmp != 0) {
			return cmp;
		}
		return this.value.compareTo(o.value);
	}

	@Override
	public int hashCode() {
		int hash = 1;
		hash = hash * 17 + ((this.key == null) ? 0 : this.key.hashCode());
		hash = hash * 31 + ((this.value == null) ? 0 : this.value.hashCode());

		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof Pair))
			return false;

		Pair p = (Pair) obj;
		return this.key.compareTo(p.getKey()) == 0
				&& this.value.compareTo(p.getValue()) == 0;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("(").append(this.key).append(", ").append(this.value)
				.append(")");
		return sb.toString();
	}
}
