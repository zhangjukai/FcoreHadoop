package com.fcore.hadoop.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import com.fcore.hadoop.bean.CommonConstants;

public class HdfsUtil {
	// hadoop fs的配置文
	static Configuration conf = new Configuration(true);
	static {
		conf.set("fs.defaultFS", CommonConstants.DEFAULT_FS);
		System.setProperty("HADOOP_USER_NAME", CommonConstants.HADOOP_USER_NAME);
	}

	/**
	 * 上传本地文件到HDFS
	 * 
	 * @param local
	 *            本地文件
	 * @param remote
	 *            远程文件
	 * @throws Exception
	 */
	public static void uploadLocalFileToHDFS(String local, String remote) {
		
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(conf);
			fileSystem.getConf().set("dfs.blocksize","67108864");
			Path localPath = new Path(local);
			Path remotePath = new Path(remote);
			Long start = System.currentTimeMillis();
			fileSystem.copyFromLocalFile(false, localPath, remotePath);
			System.out.println("Time:" + (System.currentTimeMillis() - start));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fileSystem != null) {
				IOUtils.closeStream(fileSystem);
			}
		}
	}

	/**
	 * 通过流的方式上传文件到HDFS
	 * @param in 文件流
	 * @param hdfsPath  目标目录
	 * @throws IOException
	 */
	public static void uploadFileToHDFS(InputStream in, String hdfsPath) {
		FSDataOutputStream out = null;
		FileSystem fileSystem = null;
		try {
			final int fileLen = in.available();
			fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
			out = fileSystem.create(new Path(hdfsPath),new Progressable() {
				@Override
				public void progress() {
					try {
						System.out.println((int)((1-(double)in.available()/fileLen)*100)+"%");
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			});
			IOUtils.copyBytes(in, out, 4096, true);
			System.out.println("create file in hdfs:" + hdfsPath);
		}catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(out!=null){
				IOUtils.closeStream(out);
			}
			if(in!=null){
				IOUtils.closeStream(in);
			}
			if(fileSystem!=null){
				IOUtils.closeStream(fileSystem);
			}
		}
	}
	
	/**
	 * 下载文件到本地
	 * 
	 * @param remote
	 * @param local
	 * @throws IOException
	 */
	public static void downLoad(String remote, String local) {
		FileSystem fileSystem = null;
		try {
			Path path = new Path(remote);
			fileSystem = FileSystem.get(conf);
			fileSystem.copyToLocalFile(path, new Path(local));
			System.out.println("download: from" + remote + " to " + local);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (fileSystem != null) {
				IOUtils.closeStream(fileSystem);
			}
		}

	}

	/**
	 * 远程下载
	 * 
	 * @param remote
	 * @return
	 */
	public static byte[] downLoad(String remote) {
		FileSystem fileSystem = null;
		FSDataInputStream in = null;
		try {
			fileSystem = FileSystem.get(conf);
			in = fileSystem.open(new Path(remote));
			byte[] buffer = new byte[in.available()];
			in.read(buffer);
			return buffer;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (in != null) {
				IOUtils.closeStream(in);
			}
			if (fileSystem != null) {
				IOUtils.closeStream(fileSystem);
			}
		}
		return null;
	}

	/**
	 * 创建文件夹
	 * 
	 * @param parentPath
	 * @param dirName
	 */
	public static void mkdir(String parentPath, String dirName) {
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(conf);
			fileSystem.mkdirs(new Path(parentPath + File.separator + dirName));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fileSystem != null) {
				IOUtils.closeStream(fileSystem);
			}
		}
	}

	/**
	 * 删除问卷
	 * 
	 * @param path
	 * @return
	 */
	public static boolean delFile(String path) {
		Path dstPath = new Path(path);
		FileSystem fileSystem = null;
		try {
			fileSystem = dstPath.getFileSystem(conf);
			if (fileSystem.exists(dstPath)) {
				fileSystem.delete(dstPath, true);
			} else {
				return false;
			}
		} catch (IOException ie) {
			ie.printStackTrace();
			return false;
		} finally {
			if (fileSystem != null) {
				IOUtils.closeStream(fileSystem);
			}
		}
		return true;
	}

	public static void getDirectoryFromHdfs(String direPath) {
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(conf);
			FileStatus[] filelist = fileSystem.listStatus(new Path(direPath));
			for (int i = 0; i < filelist.length; i++) {
				System.out.println("_________________***********************____________________");
				FileStatus fileStatus = filelist[i];
				if (fileSystem.isDirectory(fileStatus.getPath())) {
					System.out.println("this file is Directory, fileName:" + fileStatus.getPath().getName()
							+ ",fileSize:" + fileStatus.getLen() + ",Permission:"+fileStatus.getPermission());
				} else if (fileSystem.isFile(fileStatus.getPath())) {
					System.out.println("this file is File, fileName:" + fileStatus.getPath().getName() + "fileSize:"
							+ fileStatus.getLen()+ ",Permission:"+fileStatus.getPermission());
				}
				System.out.println("_________________***********************____________________");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fileSystem != null) {
				IOUtils.closeStream(fileSystem);
			}
		}
	}

	public static void main(String[] args) {
		// mkdir(CommonConstants.HDFS_BASE_PATH, "test");
		// uploadLocalFileToHDFS("E:/2016/9/8/1bc51dc1-9655-49d8-8f3d-252498e25fdf.pdf","/home/hdp/hadoop/hdfs/name/test/c.txt");
		try {
			InputStream in = new BufferedInputStream(new FileInputStream("E:/2016/9/8/2a0d2ad1-21f1-4b25-b0ef-9981cdaebacf.mp4"));
			System.out.println(in.available());
			uploadFileToHDFS(in, "/home/hdp/hadoop/hdfs/name/test/b.mp4");
		} catch (Exception e) {
			e.printStackTrace();
		}
		// System.out.println(delFile(CommonConstants.HDFS_BASE_PATH+File.separator+"test"));
		//getDirectoryFromHdfs("/home/hdp/hadoop/hdfs/name/");
	}
}