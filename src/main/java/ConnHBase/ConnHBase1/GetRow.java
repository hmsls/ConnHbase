package ConnHBase.ConnHBase1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class GetRow {
	//得到一行数据
	public static void getRow(Configuration conf,String tableName,String row) throws Exception{
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		Get get = new Get(Bytes.toBytes(row));
		Result res = table.get(get);
		Cell[] cell = res.rawCells();
		for(Cell c : cell){
			//注意getFamily和getFamilyArray的区别（需要查）
			System.out.println(new String(c.getFamily(),"utf-8"));
			System.out.println(new String(c.getQualifier(),"utf-8"));
			System.out.println(new String(c.getRow(),"utf-8"));
			System.out.println(new String(c.getValue(),"utf-8"));
			System.out.println(c.getTimestamp());
		}
	}
	
	//得到全表数据
	public static void getRows(Configuration conf,String tableName) throws Exception{
		HTable table  = new HTable(conf, tableName);
		ResultScanner rs = table.getScanner(new Scan());
		Iterator<Result> it = rs.iterator();
		while(it.hasNext()){
			Result r = it.next();
			Cell[] cell = r.rawCells();
			for(Cell c : cell){
				System.out.println(new String(c.getFamily(),"utf-8"));
				System.out.println(new String(c.getQualifier(),"utf-8"));
				System.out.println(new String(c.getRow(),"utf-8"));
				System.out.println(new String(c.getValue(),"utf-8"));
				System.out.println(c.getTimestamp());
			}
		}
	}
}
