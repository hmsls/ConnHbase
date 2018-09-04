package ConnHBase.ConnHBase1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertData {
	//列是动态增加的，就是添加值的时候，直接在某一个列族下指定列名，这样就创建了列，同时将值插入
	//参数qualifier就是列名，根据行、列族、列名指定唯一的一个数据cell单元格
	public static void insertData(Configuration conf,String tableName,/*String row,*/String family,String qualifier,String value) throws Exception{
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		List<Put> puts = new ArrayList<Put>();
		for(int i = 0;i<100;i++){
			Put p = new Put(Bytes.toBytes("row"+i));
			p.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value+i));
			puts.add(p);
		}
		table.put(puts);
		System.out.println("insert table done");
	}
}
