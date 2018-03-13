package com.unicom.conver;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.JOptionPane;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang3.StringUtils;

import com.unicom.model.Vac;
import com.unicom.model.VacFile;

/**
 * VAC内容格式化
 * 
 * @author 林泽涛
 * @time 2017年12月22日 下午9:13:38
 */
public class VacConvert2 {

	//文件队列容量(目前是1500个文件）
	private static final Integer MAX_FILE_QUEUE_SIZE = 3000;
	
	//处理VAC文件内容的线程数
	private static final Integer MAX_THREAD_SIZE = 8;
	
	//生成VAC文件的线程数
//	private static final Integer CREATE_FILE_THREAD_SIZE = 1;
	
//	//待处理的VAC文件名队列
//	private static BlockingQueue<File> fileQueue = 
//			new ArrayBlockingQueue<>(MAX_FILE_QUEUE_SIZE);
//	
//	//待重新生成的VAC文件队列
//	private static BlockingQueue<VacFile> creat_file_queue =
//			new ArrayBlockingQueue<>(MAX_FILE_QUEUE_SIZE);
	
	//处理内容的线程池
	private static ExecutorService handle_poor =
			Executors.newFixedThreadPool(MAX_THREAD_SIZE);
	
	
	private static final String THREE_ZERO = "000";
	
	//当前路径（兼容Windows和Linux)
	private static String currentPath = System.getProperty("user.dir");
	
	//换行（兼容Windows和Linux)
	private static final String SEPRATOR = File.separator;
	
	//VAC文件内容记录字段与字段之间用Tab分隔
	private static String SPACE = "\t";
	
	//计数器（线程安全）
	private static AtomicInteger atomicInteger = new AtomicInteger(1);
	
	//待处理的VAC文件数量
	private static Integer bigFileSzie = 0;
	
	/**
	 * 构造方法
	 * @param _currentPath 后期可以拓展为用户自定义的路径
	 */
	public VacConvert2(String _currentPath) {
		super();
		currentPath = _currentPath;
	}

	/**
	 * 打印相关信息
	 */
	public static void printInfo() {
		System.out.println();
		System.out.println("====================================处理VAC文件信息====================");
		System.out.println("当前路径:"+currentPath);
		System.out.println("处理VAC文件的线程数:"+MAX_THREAD_SIZE);
//		System.out.println("生成VAC文件的线程数:"+CREATE_FILE_THREAD_SIZE);
		System.out.println("当前匹配到的VAC大文件数量:"+bigFileSzie);
		System.out.println("===================================================================");
		System.out.println();
	}
	
	/**
	 * 本类测试
	 * @param args
	 */
	public static void main(String[] args) {
		String outPath = "C:\\Users\\linzt8\\Desktop\\华为\\VAC\\SubscribeInfo18 - 副本";
		handleVAC(outPath);
	}
	
	/**
	 * 调用入口
	 * @time 2017年12月24日
	 * @param outPath
	 */
	public static void handleVAC(String _outPath) {
		try {
			if(StringUtils.isBlank(_outPath)) {
				_outPath = System.getProperty("user.dir");
			}
			currentPath = _outPath;
			VacConvert2 vacConver = new VacConvert2(_outPath);
			if(StringUtils.isBlank(currentPath)) {
				JOptionPane.showMessageDialog(null, "VAC文件初始父路径不能为空！");  
				return;
			}
			Collection<File> listFiles = vacConver.getVacFileList();
			if(listFiles.isEmpty()) {
				JOptionPane.showMessageDialog(null, currentPath+",该路径下没有VAC文件：");  
				return;
			}
			bigFileSzie = listFiles.size();
			printInfo();
			
			vacConver.refreshFileQueue(listFiles);//同步待处理的文件队列
			
			Thread.sleep(30000);
			vacConver.shutDownPoors();//为线程池中的线程分配任务
			
			System.out.println("VAC主线程执行完成！");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 为线程池中的线程分配任务
	 * @time 2017年12月23日
	 */
	public void shutDownPoors() {
//		int fileSiz = fileQueue.size();
//		for(int i=0;i<fileSiz;i++) {
//			try {
//				//往线程池添加任务：处理Vac内容
//				handle_poor.submit(new FileFormator(fileQueue.take()));
//				//往线程池添加任务： 生成新Vac文件
//				System.out.println("prepare for getting a vacFile from Queue...");
//				handle_poor.submit(new FileCreator(creat_file_queue.take()));
//				System.out.println("getted a vacFile from Queue!");
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
		handle_poor.shutdown();
		System.out.println("为线程池中的线程分配任务完成！");
	}
	
	
	/**
	 * 获取初始路径
	 */
	public  Collection<File> getVacFileList(){
		File file = new File(currentPath);
		Collection<File> listFiles = FileUtils.listFiles(file, FileFilterUtils.prefixFileFilter("SubscribeInfo"), null);
		for (File f : listFiles) {
            System.out.println("getFile====="+f.getName());
        }
		return listFiles == null ? Collections.EMPTY_LIST : listFiles ;
	}
	
	/**
	 * 同步待处理的文件队列
	 */
	public void refreshFileQueue(Collection<File> listFiles) {
		if(!listFiles.isEmpty()) {
			listFiles.forEach(file -> {
				try {
//					fileQueue.put(file);
					handle_poor.submit(new FileFormator(file));
					System.out.println("submit a vac file to handle_poor...");
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		}
	}
	
	/**
	 * 返回处理后的Vac文件（已封装成VacFile对象）
	 * @time 2017年12月23日
	 * @return
	 */
	public static VacFile getHandledVacFile(File file) {
		List<String> lines = null;
		VacFile vacFile = null;
		try {
			lines = FileUtils.readLines(file, "UTF-8");
			if(lines == null) {
				return new VacFile();
			}
			String firstRow = lines.get(0);
			int size = lines.size();
			List<Vac> dataList = new ArrayList<>(size);
//			lines.subList(1, size).parallelStream().forEach(record -> {
			lines.subList(1, size).forEach(record -> {
				String[] arr = record.split(SPACE);
				Vac vac = getHandleVac(arr);
				dataList.add(vac);
			});
			String fileName = currentPath+SEPRATOR+"result"+SEPRATOR+"vac"+SEPRATOR+file.getName();
			firstRow = getFirstRowString(firstRow.trim(),dataList.size());
			vacFile = new VacFile(fileName,firstRow,dataList);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return vacFile;
	}
	
	/**
	 * 格式化首行信息
	 * @time 2017年12月24日
	 * @param _firstRow
	 * @param index
	 * @return
	 */
	public static String getFirstRowString(String firstRow ,int index ) {
		//0002000002017120101010290219700101000000209901010000000000000001
		String indexString = String.format("%04d", index);
		firstRow = firstRow.trim().substring(0, firstRow.length()-4);
		return (firstRow + indexString);
	}
	
	/**
	 * 将每行封装成一个Vac记录
	 * @time 2017年12月23日
	 * @param arr 每行分割后的列数组
	 * @return
	 */
	public static Vac getHandleVac(String[] arr) {
		Vac vac = new Vac();
		vac.setRecordSquenceID(arr[0]);
		vac.setUserIdType(arr[1]);
		vac.setUserId(StringUtils.startsWith(arr[2], "86") ? arr[2].substring(2) : arr[2]);
		vac.setServiceType(arr[3]);
		vac.setSpId(arr[4]+THREE_ZERO);
		vac.setSpProductId(arr[5]);
		vac.setUpdateType(arr[6]);
		vac.setUpdateTime(arr[7]);
		vac.setUpdateDesc(arr[8]);
		vac.setLinkID(arr[9]);
		vac.setContent(arr[10]);
		vac.setEffectivDate(arr[11]);
		vac.setExpireDate(arr[12]);
		vac.setTimeStamp(arr[13]);
		if(arr.length==15) {
			vac.setEncodeStr(arr[14]);
		}else {
			vac.setEncodeStr("");
		}
		return vac;
	}
	
	
	/**
	 * 静态内部类：专门处理Vac文件的线程
	 * @author 林泽涛
	 * @time 2017年12月23日 下午5:20:08
	 */
	static class FileFormator implements Runnable{
		
		private File file ;
		
		public FileFormator(File file) {
			super();
			this.file = file;
		}

		@Override
		public void run(){
			try {
				//格式化VAC文件内容
				VacFile vacFile = getHandledVacFile(file);
				
				//放到待生成新文件的列队中
				Optional.of(vacFile);//vacFile不能为null
//				creat_file_queue.put(vacFile);
//				handle_poor.submit(new FileCreator(vacFile));
				
				long start = System.currentTimeMillis();
				File file = new File(vacFile.getFileName());
				String data = getVacDataString(vacFile);
				FileUtils.write(file, data,Charset.defaultCharset());
				long end = System.currentTimeMillis();
				System.out.println("create vac file ,cost "+(end-start)+", index:"+atomicInteger.getAndIncrement());
				
				System.out.println("submit a vacFile to handle_poor...");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 静态内部类：专门生成Vac文件的线程
	 * @author 林泽涛
	 * @time 2017年12月23日 下午5:20:08
	 */
	static class FileCreator implements Runnable{
		
		private VacFile vacFile ;
		
		public FileCreator(VacFile vacFile) {
			super();
			this.vacFile = vacFile;
		}

		@Override
		public void run(){
			try {
				//生成Vac文件到指定目录下
				long start = System.currentTimeMillis();
				File file = new File(vacFile.getFileName());
				String data = getVacDataString(vacFile);
				FileUtils.write(file, data,Charset.defaultCharset());
				long end = System.currentTimeMillis();
				System.out.println("create vac file ,cost "+(end-start)+", index:"+atomicInteger.getAndIncrement());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 获取一个Vac文件的所有数据（缓存）
	 * @time 2017年12月23日
	 * @param list
	 * @return
	 */
	public static String getVacDataString(VacFile vacFile) {
		List<Vac> dataList = vacFile.getDataList();
		if(dataList==null) return "";
		StringBuffer sb = new StringBuffer();//线程安全
		sb.append(vacFile.getFirstRow()).append(System.lineSeparator());
		dataList.forEach(vac -> {
			  sb.append(vac.getRecordSquenceID()).append(SPACE)
				.append(vac.getUserIdType()).append(SPACE)
				.append(vac.getUserId()).append(SPACE)
				.append(vac.getServiceType()).append(SPACE)
				.append(vac.getSpId()).append(SPACE)
				.append(vac.getSpProductId()).append(SPACE)
				.append(vac.getUpdateType()).append(SPACE)
				.append(vac.getUpdateTime()).append(SPACE)
				.append(vac.getUpdateDesc()).append(SPACE)
				.append(vac.getLinkID()).append(SPACE)
				.append(vac.getContent()).append(SPACE)
				.append(vac.getEffectivDate()).append(SPACE)
				.append(vac.getExpireDate()).append(SPACE)
				.append(vac.getTimeStamp()).append(SPACE)
				.append(vac.getEncodeStr()).append(SPACE)
				.append(System.lineSeparator());
		});
		return sb.toString();
	}
	
	
}
