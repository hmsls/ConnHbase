package ConnHDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class FindFileOnHDFS {
	public static void main(String[] args) throws Exception {
//		getHDFSNodes();
		getFileLocal();
	}

	public static void getFileLocal() throws Exception {

		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path fpath = new Path("/user/lishuai/hdfs-audit.log");

		FileStatus fileStatus = hdfs.getFileStatus(fpath);
		BlockLocation[] blkLocations = hdfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

		int blockLen = blkLocations.length;

		for (int i = 0; i < blockLen; ++i) {
			String[] hosts = blkLocations[i].getHosts();
			System.out.println("block_" + i + "_location:" + hosts[i]);
		}
	}

	public static void getHDFSNodes() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

		for (int i = 0; i < dataNodeStats.length; ++i) {
			System.out.println("DataNode_" + i + "_Node:" + dataNodeStats[i].getHostName());
		}
	}
}
