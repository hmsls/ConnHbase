package ConnHBase.ConnHBase1;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTbale {
	public static void create(Configuration conf,String tableName,List<String> columnsFamily) throws Exception{
		HBaseAdmin hadmin = new HBaseAdmin(conf);
		if(isTableExist(conf, tableName)){
			System.out.println("表" + tableName + "已经存在");
		}
		else{
			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
			for(String f : columnsFamily){
				HColumnDescriptor colDesc = new HColumnDescriptor(Bytes.toBytes(f));
				desc.addFamily(colDesc);
			}
			hadmin.createTable(desc);
			
			System.out.println("创建表成功");
		}
	}
	
	//判断表是否存在
	public static boolean isTableExist(Configuration conf,String tableName) throws Exception{
		Boolean flag = false;
		HBaseAdmin hadmin = new HBaseAdmin(conf);
		flag = hadmin.tableExists(tableName);
		System.out.println(flag);
		return flag;
	}
	
	//动态添加列族
	public static void addColFamily(Configuration conf,String tableName,List<String> columnsFamily) throws Exception{
		HBaseAdmin hadmin = new HBaseAdmin(conf);
		for(String f : columnsFamily){
			//这里增加列，但是实际上还是增加了列族，意义是建表后，在之前存在的列族基础上增加列族。
			//在增加列族的时候，列族名称是按照字典排序的，如，创建f1-f10的列族，排序为f1,f10,f2,f3,f4...
			hadmin.addColumn(tableName, new HColumnDescriptor(f));
		}
		System.out.println("插入列族成功");
	}
}
