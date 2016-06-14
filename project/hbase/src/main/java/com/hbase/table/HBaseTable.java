package com.hbase.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTable {

	private static Configuration conf = null;
	private static HBaseAdmin admin;
	private static HTable table;

	static {
		conf = HBaseConfiguration.create();
	}

	@SuppressWarnings("deprecation")
	public static void creatTable(String tableName, String[] familys)
			throws Exception {
		admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("Table already exists!");
		} else {
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDescriptor.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDescriptor);
			System.out.println("Create table " + tableName + " ok.");
		}
	}


	public static void deleteTable(String tableName) throws Exception {
		try {
			admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("Delete table " + tableName + " ok.");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}

	public static void addRecord(String tableName, String rowKey,
			String family, String qualifier, String value) throws Exception {
		try {
			table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(put);
			System.out.println("Insert recored " + rowKey + " to table "
					+ tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void deleteRecord(String tableName, String rowKey)
			throws IOException {
		table = new HTable(conf, tableName);
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		table.delete(list);
		System.out.println("Delete recored " + rowKey + " ok.");
	}

	@SuppressWarnings("deprecation")
	public static void getOneRecord(String tableName, String rowKey)
			throws IOException {
		table = new HTable(conf, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw()) {
			System.out.print(new String(kv.getRow()) + " ");
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + " ");			
			System.out.println(new String(kv.getValue()));
		}
	}

	@SuppressWarnings("deprecation")
	public static void getAllRecord(String tableName) {
		try {
			table = new HTable(conf, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " ");					
					System.out.println(new String(kv.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] agrs) {
		try {
			String tableName = "scores";
			String[] columns = { "grade", "course" };
			//HBaseTable.deleteTable(tableName);
			HBaseTable.creatTable(tableName, columns);

			// add record record1
			HBaseTable.addRecord(tableName, "record1", "grade", "", "92");
			HBaseTable.addRecord(tableName, "record1", "course", "bigdata", "");
			
			// add record record2
			HBaseTable.addRecord(tableName, "record2", "grade", "", "89");
			HBaseTable.addRecord(tableName, "record2", "course", "math", "");

			System.out.println("===========Get one record========");
			HBaseTable.getOneRecord(tableName, "record1");

			System.out.println("===========Show all record========");
			HBaseTable.getAllRecord(tableName);

			System.out.println("===========Delete one record========");
			//HBaseTable.deleteRecord(tableName, "record2");
			HBaseTable.getAllRecord(tableName);

			System.out.println("===========Show all record========");
			HBaseTable.getAllRecord(tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}