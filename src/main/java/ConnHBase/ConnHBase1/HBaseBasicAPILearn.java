package ConnHBase.ConnHBase1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.taglibs.standard.lang.jstl.test.beans.PublicBean1;
public class HBaseBasicAPILearn {
	/**
	 * 这是客户端缓冲区测试类。是把客户端的put操作先缓存到内存中，直到flush的时候，在
	 * RPC到远程服务端，进行相关操作。
	 */
	public static void testClientFlust() throws Exception{
		HTable table = new HTable(new CreateHBaseConnection().createHBaseConfiguration(), TableName.valueOf("testFlush"));
		//判断是否是自动刷新
		System.out.println("Auto flush : "+table.isAutoFlush());
		//设置自动刷写
		table.setAutoFlush(false, false);
		
		//put数据，第一行
		Put put = new Put(Bytes.toBytes("row1"));
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
		table.put(put);
		//put数据，第二行
		Put put2 = new Put(Bytes.toBytes("row2"));
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("q2"),Bytes.toBytes("val2"));
		table.put(put);
		//put数据，第三行
		Put put3 = new Put(Bytes.toBytes("val3"));
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("q3"), Bytes.toBytes("val3"));
		table.put(put);
		
		//在没有flush的时候，查询第一行
		Get get = new Get(Bytes.toBytes("row1"));
		Result res = table.get(get);
		//注意，下面的会得到空的值，因为没有数据，所以会报空指针的异常，直接打印res即可
//		byte[] bys = res.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("q1"));
		System.out.println(res);
		
		//访问客户端写缓冲区中的内容
		System.out.println("-----访问客户端写缓冲区中的内容-----");
		List<Row> rows = table.getWriteBuffer();
		Iterator it = rows.iterator();
		while(it.hasNext()){
			Row row = (Row) it.next();
			System.out.println(row);
		}
		
		//手动进行数据刷写put，然后通过put的RPC执行操作。
		table.flushCommits();
		System.out.println("-------------------已经手动刷新-------------------");
		
		Result res2 = table.get(get);
//		byte[] bys2 = res2.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("q1"));
		System.out.println(res2);
		
		table.close();
	}
	
	/**
	 * 测试原子性操作
	 * @throws Exception 
	 */
	public static void testAtomicity() throws Exception{
		HTable  table = new HTable(new CreateHBaseConnection().createHBaseConfiguration(), TableName.valueOf("testFlush"));
		//创建一个put实例
		Put put1 = new Put(Bytes.toBytes("row4"));
		put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("q4"), Bytes.toBytes("val4"));
		//检查指定列是否存在，安检查的结果决定是否执行put操作
		boolean res = table.checkAndPut(Bytes.toBytes("row4"), Bytes.toBytes("cf1"), Bytes.toBytes("q4"), Bytes.toBytes("val4"), put1);
		System.out.println("Put applied : "+res);
		
		//再次向同一个单元格写入数据
		boolean res2 = table.checkAndPut(Bytes.toBytes("row4"), Bytes.toBytes("cf1"), Bytes.toBytes("q4"), Bytes.toBytes("val4"), put1);
		System.out.println("Put applied : "+res2);
		
		//创建一个新的put，这次使用一个新的列限定符
		Put put2 = new Put(Bytes.toBytes("row4"));
		put2.add(Bytes.toBytes("cf1"), Bytes.toBytes("q5"), Bytes.toBytes("val5"));
		boolean res3 = table.checkAndPut(Bytes.toBytes("row4"), Bytes.toBytes("cf1"), Bytes.toBytes("q4"), Bytes.toBytes("val4"), put2);
		System.out.println("Put applied : "+res3);
		
		//再创建一个put实例，这次使用一个不同的行健
		Put put3 = new Put(Bytes.toBytes("row5"));
		put3.add(Bytes.toBytes("cf1"), Bytes.toBytes("q4"), Bytes.toBytes("val6"));
		//这块会抛出异常
		boolean res4 = table.checkAndPut(Bytes.toBytes("row4"), Bytes.toBytes("cf1"), Bytes.toBytes("q4"), Bytes.toBytes("val4"), put3);
		System.out.println("Put applied : "+res4);
		
		table.close();
	}
	
	/**
	 * 使用get得到一行数据
	 * @throws Exception 
	 */
	public static void testGetOneRow() throws Exception{
		HTable table = new HTable(new CreateHBaseConnection().createHBaseConfiguration(), TableName.valueOf("testFlush"));
		//使用行健来创建一个Get实例
		Get get = new Get(Bytes.toBytes("row1"));
		get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("q1"));
		Result result = table.get(get);
		byte[] bys = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("q1"));
		System.out.println(Bytes.toString(bys));
		
		table.close();
	}
	/**
	 * 使用get得到多行数据
	 * @throws Exception 
	 */
	public static void testGetMulRow() throws Exception{
		HTable table = new HTable(new CreateHBaseConnection().createHBaseConfiguration(), TableName.valueOf("testFlush"));
		byte[] cf1 = Bytes.toBytes("cf1");
		byte[] q1 = Bytes.toBytes("q1");
		byte[] q2 = Bytes.toBytes("q2");
		byte[] row1 = Bytes.toBytes("row1");
		byte[] row2 = Bytes.toBytes("row2");
		
		List<Get> gets = new ArrayList<Get>();
		Get get1 = new Get(row1);
		get1.addColumn(cf1, q1);
		gets.add(get1);
		
		Get get2 = new Get(row2);
		get2.addColumn(cf1, q2);
		gets.add(get2);
		
		Result[] results = table.get(gets);
		System.out.println("First Iterator.......");
		for(Result result : results){
			String row = Bytes.toString(result.getRow());
			System.out.println("Row : "+ row + " ");
			byte[] val = null;
			if(result.containsColumn(cf1, q1)){
				val  = result.getValue(cf1, q1);
				System.out.println("Value : "+ Bytes.toString(val));
			}
			if(result.containsColumn(cf1, q2)){
				val = result.getValue(cf1, q2);
				System.out.println("Value : "+ Bytes.toString(val));
			}
		}
		
		System.out.println("Second Iterator........");
		for(Result result : results){
			for(KeyValue kv : result.raw()){
				System.out.println("Row : "+ Bytes.toString(kv.getRow()) + "    " + "Value : "+ Bytes.toString(kv.getValue()));
			}
		}
		
		table.close();
	}
	
	/**
	 * 使用特殊检索方法。
	 * @throws Exception 
	 */
	public static void testCheck() throws Exception{
		HTable table = new HTable(new CreateHBaseConnection().createHBaseConfiguration(), TableName.valueOf("testFlush"));
		
		Result res1 = table.getRowOrBefore(Bytes.toBytes("row1"), Bytes.toBytes("cf1"));
		System.out.println("Found : "+ Bytes.toString(res1.getRow()));
		//尝试查找不存在的列
		Result res2 = table.getRowOrBefore(Bytes.toBytes("row112"),Bytes.toBytes("cf1"));
		System.out.println("Found : "+ Bytes.toString(res2.getRow()));
		
		for(KeyValue kv : res2.raw()){
			System.out.println("Columns : "+ Bytes.toString(kv.getFamily())+ "    "+ Bytes.toString(kv.getQualifier())+ "    "
		+Bytes.toString(kv.getValue()));
		}
		//查找前一行
		Result res3 = table.getRowOrBefore(Bytes.toBytes("row99"), Bytes.toBytes("cf1"));
		System.out.println("Found : "+ res3);
		
		table.close();
	}

	/**
	 * delete操作，也可以用list放置很多Delete操作。然后统一执行
	 * @throws Exception 
	 */
	public static void testDelete() throws Exception{
		HTable table = new HTable(new CreateHBaseConnection().createHBaseConfiguration(), TableName.valueOf("testFlush"));
		Delete delete = new Delete(Bytes.toBytes("row20"));
		delete.setTimestamp(1);
		delete.deleteColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col"),1);
		delete.deleteColumns(Bytes.toBytes("cf2"), Bytes.toBytes("q2"));
		delete.deleteColumns(Bytes.toBytes("cf2"), Bytes.toBytes("q2"),15);
		delete.deleteFamily(Bytes.toBytes("cf4"));
		delete.deleteFamily(Bytes.toBytes("cf4"),3);
		
		table.delete(delete);
		table.close();
	}
	
	/**
	 * delete操作，通过checkAndDelete进行操作
	 * @throws Exception 
	 */
	public static void testCheckAndDelete() throws Exception{
		HTable table = new HTable(new CreateHBaseConnection().createHBaseConfiguration(), TableName.valueOf("testFlush"));
		Delete delete = new Delete(Bytes.toBytes("row30"));
		delete.deleteColumn(Bytes.toBytes("cf1"), Bytes.toBytes("q2"));
		//检查指定列是否存在，依据检查结果进行删除
		boolean res1 = table.checkAndDelete(Bytes.toBytes("row30"), Bytes.toBytes("cf1"), Bytes.toBytes("q2"), Bytes.toBytes("tom30"), delete);
		System.out.println("Delete Successful ...." + res1);
		
		table.close();
	}

	/**
	 * 使用批处理操作batch()
	 * @throws Exception 
	 */
	public static void testBatch() throws Exception{
		HTable table = new HTable(new CreateHBaseConnection().createHBaseConfiguration(), TableName.valueOf("testFlush"));
		byte[] row1 = Bytes.toBytes("row1");
		byte[] row2 = Bytes.toBytes("row2");
		byte[] cf1 = Bytes.toBytes("cf1");
		byte[] cf2 = Bytes.toBytes("cf2");
		byte[] q1 = Bytes.toBytes("q1");
		byte[] q2 = Bytes.toBytes("q2");
		
		List<Row> batch = new ArrayList<Row>();
		
		Put put = new Put(row2);
		put.add(cf2,q1,Bytes.toBytes("val5"));
		batch.add(put);
		
		Get get = new Get(row2);
		get.addColumn(cf1, q1);
		batch.add(get);
		
		Delete delete = new Delete(row1);
		delete.deleteColumn(cf1, q2);
		batch.add(delete);
		
		Get get2 = new Get(row2);
		get2.addFamily(Bytes.toBytes("BOGUS"));
		batch.add(get2);
		
		Object[] result = new Object[batch.size()];
		try{
			//把各种操作放到一个集合中，使用batch方法批量执行
			table.batch(batch, result);
		}catch(Exception e){
			System.out.println("Error : "+ e);
		}
		
		for(int i = 0;i<result.length;i++){
			System.out.println("Result [" + i + "]" + result[i]);
		}
		
		table.close();
	}

	/**
	 * 注意，现在已经不支持用户级别的行锁了，在HTable中已经没有了lockRow方法
	 * 行锁，在不必要的时候，不要使用行锁，如果必须使用，一定要节约占用锁的时间
	 * 一般默认为1分钟，可以通过hbase-site.xml中hbase.regionserver.lease.period设置时间
	 * 该锁阻塞所有并发读取
	 * @throws Exception 
	 */
	public static void testRowLock() throws Exception{
		class UnlockedPut implements Runnable{
			public void run(){
				try{
					HTable table = new HTable(new CreateHBaseConnection().createHBaseConfiguration(), TableName.valueOf("testFlush"));
					Put put = new Put(Bytes.toBytes("row1"));
					put.add(Bytes.toBytes("cf1"), Bytes.toBytes("q1"), Bytes.toBytes("tomes"));
					Long time = System.currentTimeMillis();
					System.out.println("Thread trying to put same row now....");
					table.put(put);
					System.out.println("Wati time : "+(System.currentTimeMillis() - time) + "ms");
					table.close();
				}catch (Exception e){
					System.out.println("Error " + e);
				}
				//获取行锁
				System.out.println("Taking out lock ...");
			}
		}
	}

	/**
	 * 测试扫描
	 * @throws Exception 
	 */
	public static void testScanner() throws Exception{
		HTable table = new HTable(new CreateHBaseConnection().createHBaseConfiguration(), TableName.valueOf("testFlush"));
		//创建一个空的scan实例
		Scan scan = new Scan();
		//取得一个扫描器迭代所有的行，ResultScanner把每一行的数据封装成一个Result实例
		//扫描全表内容
		ResultScanner scanner1 = table.getScanner(scan);
		for(Result res : scanner1){
			System.out.println(res);
		}
		scanner1.close();
		
		Scan scan1 = new Scan();
		//扫描指定列族
		scan1.addFamily(Bytes.toBytes("cf1"));
		ResultScanner scanner2 = table.getScanner(scan1);
		for(Result res : scanner2){
			System.out.println(res);
		}
		scanner2.close();
		
		Scan scan2 = new Scan();
		//使用builder模式将详细限制条件添加到scan中
		//扫描指定列和行范围
		scan2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("q1")).
			addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("q1")).
			setStartRow(Bytes.toBytes("row50")).
			setStopRow(Bytes.toBytes("row60"));
		ResultScanner scanner3 = table.getScanner(scan2);
		for(Result res : scanner3){
			System.out.println(res);
		}
		scanner3.close();
		
		table.close();
	}

	/**
	 * 在扫描中使用缓存和批量参数
	 * RPCs = Result个数  /  缓存个数   +  1
	 * 其中，Result个数为单元格个数，如，有2个列族，每个列族10列，20行的表，有200个result（200列，因为每个
	 * 		     列只有一个版本）
	 * 公式：RPC请求次数 = （行数 * 每行的列数） / Min（每行的列数，批量大小） / 扫描器缓存  ，然后要考虑一些请求来
	 * 打开和关闭扫描器
	 * @throws Exception 
	 */
	public static void testScanCacheAndBatch(int cache,int batch) throws Exception{
		HTable table = new HTable(new CreateHBaseConnection().createHBaseConfiguration(), TableName.valueOf("testFlush"));
		Logger log = Logger.getLogger("org.apache.hadoop");
		final int[] counters = {0,0};
		Appender appender = new AppenderSkeleton() {
			@Override
			protected void append(LoggingEvent event) {
				String msg = event.getMessage().toString();
				if(msg != null && msg.contains("Call: next")){
					counters[0]++;
				}
			}
			public void close(){
				
			}
			public boolean requiresLayout() {
				return false;
			}
		};
		log.removeAllAppenders();
		log.setAdditivity(false);
		log.addAppender(appender);
		log.setLevel(Level.DEBUG);
		
		Scan scan = new Scan();
		//设置缓存和批量处理两个参数
		scan.setCaching(cache);
		scan.setBatch(batch);
		ResultScanner scanner = table.getScanner(scan);
		for(Result res : scanner){
			counters[1]++;
		}
		scanner.close();
		System.out.println("Caching : "+cache+"****"+"Batch : "+batch+"****"+"Results : "+counters[1]+"****"+"RPCs : "+counters[0]);
		
		table.close();
	}
}
