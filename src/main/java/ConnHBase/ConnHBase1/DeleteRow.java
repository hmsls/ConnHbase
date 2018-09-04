package ConnHBase.ConnHBase1;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteRow {
	//删除单行
	public static void delRow(Configuration conf,String tableName,String row) throws Exception{
		HTable table = new HTable(conf, tableName);
		Delete del = new Delete(Bytes.toBytes(row));
		table.delete(del);
		System.out.println("delete single row done");
	}
	//批量删除行
	public static void delRows(Configuration conf,String tableName,List<String> rows) throws Exception{
		HTable table = new HTable(conf, tableName);
		for(String r : rows){
			Delete del = new Delete(Bytes.toBytes(r));
			table.delete(del);
		}
		System.out.println("delete rows done");
	}
}
