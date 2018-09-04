package ConnHBase.ConnHBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
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


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CreateTable {
	
	//创建HBase的HbaseConfiguration，但实际上还是用的Hadoop的Configuration
	public static Configuration createHBaseConfiguration(){
		Configuration hconf = HBaseConfiguration.create();
		hconf.addResource("hbase-site.xml");
		return hconf;
	}
	
	//创建Hadoop的配置，Configuration
	public static Configuration createHadoopConfiguration(){
		Configuration conf = new Configuration();
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("hbase-site.xml");
		return conf;
	}
	
    public static void main(String args[]) throws Exception {
//        HBaseConfiguration hc = new HBaseConfiguration();
//        hc.addResource(conf);
//        hc.set("hbase.zookeeper.quorum","");
//        hc.set("hbase.zookeeper.property.clientPort","2181");
        //判断表是否存在
//    	isExist(createHadoopConfiguration(), "ls_table_test");
    	//建表
//    	ArrayList<String> cf = new ArrayList<String>();
//    	cf.add("c1");
//    	cf.add("c2");
//    	cf.add("c3");
//    	cf.add("c4");
//    	create(createHadoopConfiguration(), "h_test1", cf);
    	//删表
//    	deleteTable(createHadoopConfiguration(), "ls_table_test");
    	//添加数据,单条添加
    	addRow(createHadoopConfiguration(), "h_test1", "r1", "c1", "c11", "r1c11");
    	addRow(createHadoopConfiguration(), "h_test1", "r2", "c2", "c21", "r2c21");
    	addRow(createHadoopConfiguration(), "h_test1", "r3", "c3", "c31", "r3c31");
    	addRow(createHadoopConfiguration(), "h_test1", "r4", "c4", "c41", "r4c41");
    	//删除一行
//    	delRow(createHadoopConfiguration(), "h_test1","r1");
    	//批量删除行
//    	ArrayList<String> rows = new ArrayList<String>();
//    	rows.add("r1");
//    	rows.add("r2");
//    	rows.add("r3");
//    	rows.add("r4");
//    	delMultiRows(createHadoopConfiguration(), "h_test1", rows);
    	//得到一行数据
//    	getRow(createHadoopConfiguration(), "h_test1", "r1");
    	//得到所有的数据
    	getAllRow2(createHadoopConfiguration(), "h_test1");
    }
    
  //判断表是否存在
    public static boolean isExist(Configuration conf,String tableName) throws IOException{
    	Boolean flag;
    	HBaseAdmin admin = new HBaseAdmin(conf);
    	flag = admin.tableExists(tableName);
    	System.out.println(flag);
    	return flag;
    }
    
  //建表
    public static void create(Configuration conf,String tableName, List<String> columnsFamily) throws IOException {
    	//HBaseAdmin 管理表信息。包括：创建表,删除表,列出表 项,使表有效或无效,以及添加或删除表列族成员（列，不是列族）等。
        HBaseAdmin hadmin = new HBaseAdmin(conf);
        if(hadmin.tableExists(tableName)){
        	System.out.println("表" + tableName + "已经存在");
        	return;
        }
        else{
        	HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        	for(String f : columnsFamily){
        		tableDesc.addFamily(new HColumnDescriptor(f));
        	}
        	hadmin.createTable(tableDesc);
        }
        System.out.println("create table done");
    }
    
    //插入数据
    public static void insert(Configuration conf,String tableName) throws Exception{
    	byte[] row = new byte[]{'r'};
    	byte[] tn = Bytes.toBytes(tableName);
//    	HTable table = new HTable(conf,tn);
    	Put p = new Put(row);
    	p.add(Bytes.toBytes("cf1"),Bytes.toBytes("111"),Bytes.toBytes("lishuailishuai"));
    	System.out.println("insert into table done");
    }
    
  //删表
    public static void deleteTable(Configuration conf,String tableName) throws Exception{
    	HBaseAdmin hadmin = new HBaseAdmin(conf);
    	if(hadmin.tableExists(tableName)){
    		hadmin.disableTable(tableName);
    		hadmin.deleteTable(tableName);
    		System.out.println("delete table done");
    	}else{
    		System.out.println("表" + tableName + " 不存在");
    	}
    }
    
  //添加数据,单条添加
    public static void addRow(Configuration conf,String tableName,String row,String columnFamily,String column,String value) throws Exception{
    	HTable table = new HTable(conf, tableName);
    	Put put = new Put(Bytes.toBytes(row));
    	put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
    	table.put(put);
    }
    
  //删除一行
    public static void delRow(Configuration conf,String tableName,String row) throws Exception{
    	HTable table = new HTable(conf, tableName);
    	Delete del = new Delete(Bytes.toBytes(row));
    	table.delete(del);
    	System.out.println("delete single row done");
    }
    
  //批量删除行
    public static void delMultiRows(Configuration conf,String tableName,List<String> row) throws Exception{
    	HTable table = new HTable(conf, tableName);
    	for(String r : row){
    		Delete del = new Delete(Bytes.toBytes(r));
    		table.delete(del);
    	}
    	System.out.println("delete rows done");
    }
    
  //得到一行数据
    public static void getRow(Configuration conf,String tableName,String row) throws Exception{
    	HTable table = new HTable(conf,tableName);
    	Get get = new Get(Bytes.toBytes(row));
    	Result res = table.get(get);
    	KeyValue[] rowKv = res.raw();
    	for(KeyValue kv : rowKv){
    		System.out.println(new String(kv.getFamily()));
    		System.out.println(new String(kv.getQualifier()));
    		System.out.println(new String(kv.getRow()));
    		System.out.println(new String(kv.getValue()));
    		System.out.println(kv.getTimestamp());
    		System.out.println("-----------------------------");
    	}
    }
    
     //得到所有的数据
    public static void getAllRow(Configuration conf,String tableName) throws Exception{
    	HTable table = new HTable(conf, tableName);
    	ResultScanner rs = table.getScanner(new Scan());
    	Iterator<Result> it = rs.iterator();
    	while(it.hasNext()){
    		Result res = it.next();
    		KeyValue[] rowKv = res.raw();
    		for(KeyValue kv : rowKv){
    			System.out.println(new String(kv.getFamily()));
    			System.out.println(new String(kv.getQualifier()));
    			System.out.println(new String(kv.getRow()));
    			System.out.println(new String(kv.getValue()));
    			System.out.println(kv.getTimestamp());
    			System.out.println("--------------------------------");
    		}
    	}
    }
    
    //得到所有的数据
    //注意，这个方法得出的数据应该是带有数据元信息，有乱码
    public static void getAllRow2(Configuration conf,String tableName) throws Exception{
    	HTable table = new HTable(conf, tableName);
    	ResultScanner rs = table.getScanner(new Scan());
    	Iterator<Result> it = rs.iterator();
    	while(it.hasNext()){
    		Result rowKv = it.next();
    		Cell[] cells = rowKv.rawCells();
    		for(Cell c : cells){
    			System.out.println(new String(c.getFamilyArray(),"utf-8"));
    			System.out.println(new String(c.getQualifierArray()));
    			System.out.println(new String(c.getRowArray())+" row");
    			System.out.println(new String(c.getValueArray()));
    			System.out.println(c.getTimestamp());
    		}
    	}
    }
}
