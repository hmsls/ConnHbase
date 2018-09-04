package ConnHBase.ConnHBase1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteTable {
	//删除表
	public static void deleteTable(Configuration conf,String tableName) throws Exception{
		HBaseAdmin hadmin = new HBaseAdmin(conf);
		if(CreateTbale.isTableExist(conf, tableName)){
			hadmin.disableTable(tableName);
			hadmin.deleteTable(tableName);
			System.out.println("delete table done");
		}else{
			System.out.println("表" + tableName + " 不存在");
		}
	}
	
	//删除列族
	public static void deleteFamily(Configuration conf,String tableName,String familyName) throws Exception{
		HBaseAdmin admin = new HBaseAdmin(conf);
		//即使不disable表，也可以删除列族
//		admin.disableTable(Bytes.toBytes(tableName));
		//注意这个是删除列族，不是列
		admin.deleteColumn(tableName, familyName);
		
		//以下这个方法不管用
		/*HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		desc.removeFamily(Bytes.toBytes(familyName));*/
		System.out.println("删除列族成功");
	}
}
