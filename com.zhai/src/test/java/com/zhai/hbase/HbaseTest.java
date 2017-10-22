package com.zhai.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

public class HbaseTest {
	static String tabName = "_4g_tab";
	static String columnFamily = "t";
	
	public static void createTable() throws Exception {
		HBaseDAO.createTable(tabName, columnFamily);
	}
	@Test
	public void mainTest() throws Exception {
		createTable();
	}
	
	public static void main(String[] args) throws Exception {
		createTable();
		Table tab = HBaseDAO.getHbaseConnection().getTable(TableName.valueOf(tabName));
		List<Put>puts = Lists.newArrayList(null) 
		tab.put(puts);
	}
}
