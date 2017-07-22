package com.min.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Record implements Writable {
	private int index;
	private String recordId;
	private String value;

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(index);
		out.writeUTF(recordId);
		out.writeUTF(value);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.index = in.readInt();
		this.recordId = in.readUTF();
		this.value = in.readUTF();
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public String getRecordId() {
		return recordId;
	}

	public void setRecordId(String recordId) {
		this.recordId = recordId;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "Record [index=" + index + ", recordId=" + recordId + ", value=" + value + "]";
	}
}
