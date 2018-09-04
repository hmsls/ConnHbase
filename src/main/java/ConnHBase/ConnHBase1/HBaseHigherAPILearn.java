package ConnHBase.ConnHBase1;

import java.io.IOException;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.hadoop.cli.util.RegexpComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;



public class HBaseHigherAPILearn {
	/**
	 * 使用过滤器来挑选特定的行
	 * 行过滤器依据行健来进行过滤
	 * @throws Exception 
	 */
	public static void testFilterFindRow() throws Exception{
		HTable table = new HTable(new CreateHBaseConnection().createHBaseConfiguration(), TableName.valueOf("testFlush"));
		//定义扫描器并添加行
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col"));
		//定义过滤器，使用小与等于比较，使用具体行健进行过滤，注意这个比较是按照字典顺序比较的。
		Filter filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row22")));
		//扫描器设置过滤器
		scan.setFilter(filter1);
		
		//扫描
		ResultScanner scanner1 = table.getScanner(scan);
		for(Result res : scanner1){
			System.out.println("filter1-----"+res);
		}
		scanner1.close();
		
		//设置过滤器2，使用完全等于比较，用正则表达式，只要行健中包含row字符的就找出，注意这个比较是按照字典顺序比较的。
		Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("row*"));
		scan.setFilter(filter2);
		ResultScanner scanner2 = table.getScanner(scan);
		for(Result res : scanner2){
			System.out.println("filter2-----"+res);
		}
		scanner2.close();
		
		//设置过滤器3，使用完全等于比较。使用子字符串过滤，只要行健中包含3这个字符的就找出，注意这个比较是按照字典顺序比较的。
		Filter filter3 = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("3"));
		scan.setFilter(filter3);
		ResultScanner scanner3 = table.getScanner(scan);
		for(Result res : scanner3){
			System.out.println("filter3-----"+res);
		}
		scanner3.close();
		
		table.close();
	}

	/**
	 * 列族过滤器
	 * @throws Exception 
	 */
	public static void testFilterFindColumnFamily() throws Exception{
		HTable table = new HTable(new CreateHBaseConnection().createHBaseConfiguration(), TableName.valueOf("testFlush"));
		//定义过滤器
		Filter filter1 = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("cf1")));
		//定义扫描器
		Scan scan = new Scan();
		scan.setFilter(filter1);
		ResultScanner scanner1 = table.getScanner(scan);
		for(Result res : scanner1){
			System.out.println("filter1-----"+res);
		}
		scanner1.close();
		
		Get get = new Get(Bytes.toBytes("row3"));
		get.setFilter(filter1);
		Result result = table.get(get);
		System.out.println("Result of get()-----"+result);
		
		//定义过滤器2
		Filter filter2 = new FamilyFilter(CompareFilter.CompareOp.LESS,new BinaryComparator(Bytes.toBytes("q1")));
		Get get2 = new Get(Bytes.toBytes("row5"));
		get2.addFamily(Bytes.toBytes("cf1"));
		get2.setFilter(filter2);
		Result result2 = table.get(get2);
		System.out.println("Result of get2()-----"+result2);
		
		table.close();
	}

	/**
	 * 列明过滤器
	 * @throws Exception 
	 */
	public static void testFilterFindColumn() throws Exception{
		HTable table = new HTable(new CreateHBaseConnection().createHBaseConfiguration(), TableName.valueOf("testFlush"));
		Filter filter1 = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("col")));
		Scan scan = new Scan();
		scan.setFilter(filter1);
		ResultScanner scanner1 = table.getScanner(scan);
		for(Result res : scanner1){
			System.out.println("Filter1-----"+res);
		}
		scanner1.close();
		
		Get get = new Get(Bytes.toBytes("row20"));
		get.setFilter(filter1);
		Result result = table.get(get);
		System.out.println("Result of get()-----"+result);
		
		table.close();
	}

	/**
	 * 值过滤器
	 * @throws Exception 
	 */
	public static void testFilterFindValue() throws Exception{
		HTable table = new HTable(new CreateHBaseConnection().createHBaseConfiguration(), TableName.valueOf("testFlush"));
		//定义过滤器
		Filter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("*5"));
		Scan scan = new Scan();
		scan.setFilter(filter1);
		ResultScanner scanner1 = table.getScanner(scan);
		for(Result res : scanner1){
			for(KeyValue kv : res.raw()){
				System.out.println("KeyValue---"+kv+"     "+"Value---"+"     "+Bytes.toString(kv.getValue()));
			}
		}
		scanner1.close();
		
		Get get = new Get(Bytes.toBytes("row11"));
		get.setFilter(filter1);
		Result result = table.get(get);
		for(KeyValue kv : result.raw()){
			System.out.println("KeyValue---"+kv+"     "+"Value---"+"     "+Bytes.toString(kv.getValue()));
		}
		
		table.close();
	}

	/**
	 * 使用HTablePool来共享HTable实例
	 * @throws Exception 
	 */
	public static void testHTablePool() throws Exception{
		//10表示连接池中允许最大的HTable实例数，仅仅设置实例池中能够存放的HTtableInterface的实例数
		HTablePool pool = new HTablePool(new CreateHBaseConnection().createHBaseConfiguration(),5);
		//获取10个实例，超出容量5个。
		HTableInterface[] tables = new HTableInterface[10];
		for(int n = 0;n<10 ; n++){
			tables[n] = pool.getTable("testTable");
			System.out.println(tables[n].getTableName());
		}
		
		//放回，像表实例池中返还HTable实例，其中的5个会被保留，多余的会被丢弃
		for(int m = 0;m<5;m++){
			pool.putTable(tables[m]);
		}
		
		//关闭整个表实例池，释放其中保留的表实例引用
		pool.closeTablePool("testTable");
	}
}
