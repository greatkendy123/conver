package com.unicom.filter;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang3.StringUtils;

import com.unicom.model.VacFile;
import com.unicom.util.ShowUtil;

/**
 * 对VAC文件进行filter过滤
 * 应用场景：在对VAC文件进行切割之前调用此类方法
 * 
 * @author linzt kendy
 * @time 2018年3月1日 下午2:40:40
 */
public class VacFilter {
	
	//读取文件的编码
	private static final Charset CHARSET = Charset.defaultCharset();
	
	//换行
	private static final String NEXT_LINE = System.lineSeparator();

	//VAC源文件夹路径  /4Gpositive_nopackage_67_20180201_6001_9900_all.req
//	private static final String VAC_RESOURCE_FOLDER_PATH = "C:/Users/linzt8/Desktop/华为/订购关系";
	private static final String VAC_RESOURCE_FOLDER_PATH = "C:/Users/linzt8/Desktop/华为/test";
	
	//filter源文件路径（过滤条件记录集）
	private static final String VAC_FILTER_FILE_PATH = "C:/Users/linzt8/Desktop/华为/filter.txt";
//	private static final String VAC_FILTER_FILE_PATH = "C:/Users/linzt8/Desktop/华为/测试规范.txt";
	
	//过滤条件缓存{PHONENUMBER	： VACSPPRODUCTID},大约有7万个元素
	private static Map<String,String> filterMap = new HashMap<>(70000);
	
	private  static String currentPath = "";
	
	//filter文件的第一行开头
	private static final String FIRST_ROW_CONTENT_START = "PHONENUMBER";
	
	//分隔符
	private static String SPLITOR = "\t"; //VAC文件的行记录内容是以Tab键隔开的
	
	private static final String SEPRATOR = File.separator;
	
	private static ExecutorService creatVacFilePoor = Executors.newSingleThreadExecutor();
	
	
	/**
	 * 构造方法
	 */
	public VacFilter() {
		super();
	}
	
	public VacFilter(String currentPath) {
		super();
		this.currentPath = currentPath;
	}
	
	public static void main(String... args) throws Exception{
		String outPath = "C:/Users/linzt8/Desktop/华为/VAC/test";
		handleVACFileFilter(outPath);
	}
	
	
	public static void handleVACFileFilter(String outPath) throws Exception{
	
		currentPath = StringUtils.isNotBlank(outPath) ? outPath : System.getProperty("user.dir");
		
		VacFilter VACFilter = new VacFilter();
		//获取过滤条件
		System.out.println("start to read filterVAC.txt ......");
		String path = currentPath + SEPRATOR + "filterVAC.txt";
		List<String> conditions = VACFilter.getFileList(new File(path));
		if(CollectionUtils.isEmpty(conditions)) {
			ShowUtil.show("not found filterVAC.txt, the right path is ：" + path);
			return;
		}
		//开始处理，用map结构来存储以提升检索性能，{PHONENUMBER#VACSPPRODUCTID	： null}
		filterMap.clear();
		conditions.stream()//.skip(1)
			.filter(record -> !(record.toUpperCase().startsWith(FIRST_ROW_CONTENT_START)))
			.forEach(condition -> {
				String[] rowContentArr = condition.split(SPLITOR);
				String phoneNumber = rowContentArr[0];
				String vacSpProductId =  rowContentArr[1];
				filterMap.put(phoneNumber+"#"+vacSpProductId, "");
			});
		//额外判断是否遗漏或者重复
		int conditionSize = conditions.size();
		int filterMapSize = filterMap.size();
		String compareResult = conditionSize - filterMapSize == 0 ? "yes" : "no";
		System.out.println(String.format("filter numbers ：%d  cache：%d  equals：%s", conditionSize, filterMapSize, compareResult));
		if(conditionSize != filterMapSize) {
			ShowUtil.show(String.format("error: filter record lines is %d, cache lines is %d, these two number should be equal！",conditionSize, filterMapSize));
		}
		//处理VAC
		List<File> vacFileList = VACFilter.getVACFileList();
		if(CollectionUtils.isEmpty(vacFileList)) {
			ShowUtil.show("not found VAC file, the current path is ：" + currentPath);
			return;
		}
		
		long start = System.currentTimeMillis();
		/*
		 *  处理VAC(1300个文件）
		 */
		int vacFileSize = vacFileList.size();
		for (int i = 0; i < vacFileSize; i++) {
			
			File vacFile = vacFileList.get(i);
			System.out.println(String.format("handling the %d file，VAC file name is ：%s", i, vacFile.getName()));
			List<String> vacAllContents = VACFilter.getFileList(vacFile);

			// 缓存VAC文件内容
			String firstRow = vacAllContents.get(0);
			List<String> dataList = vacAllContents.parallelStream() //并行处理更快 每个文件大概10万条记录
												 .skip(1)//跳过第一行
												 .filter(record -> { //过滤
													 	String key = getPhoneNumberWithProductId(record);
													 	return filterMap.containsKey(key) ;
												  })
												 .collect(Collectors.toList());
			
			VacFile newVACFile = new VacFile(vacFile.getAbsolutePath(), getFirstRowString(firstRow, dataList.size()));
			newVACFile.setDataListString(dataList);
			if(CollectionUtils.isEmpty(dataList)) {
				//SubscribeInfo000901152018030603210182584
				System.out.println("需要删除："+vacFile.getAbsolutePath());
				creatVacFilePoor.submit(new VACFileDeletetor(vacFile));
			}else {
				creatVacFilePoor.submit(new VACFileCreator(newVACFile));
			}
		}
		creatVacFilePoor.shutdown();
		long end = System.currentTimeMillis();
		print("handle vac file filter takes time ："+(end - start) + "ms");
	}
	
	
	/**
	 * 获取每个文件的第一行
	 */
	public static String getFirstRowString(String _firstRow,int index) {
		//8001000002017121217321209920171212173212201712122336220000990000
		//0002000002017120101010290219700101000000209901010000000000000001
		//String indexString = String.format("%07d", index);
		if(StringUtils.isBlank(_firstRow)) {
			ShowUtil.show("首行信息为空！！！");
		}else {
			_firstRow = _firstRow.trim();
			if(_firstRow.length() >= 58) {
				_firstRow = _firstRow.substring(0, 57);
			}else {
				ShowUtil.show("VAC文件首行信息长度不足58位，请找刘为敏！！！");
			}
		}
		return _firstRow + index;
	}
	
	
	/**
	 * 获取手机号与产品ID组合字符串
	 * 
	 * @param vacRowContent
	 * @return
	 */
	private static String getPhoneNumberWithProductId(String vacRowContent) {
		String[] vacRowContentArr = vacRowContent.split(SPLITOR);
		String phoneNumber = vacRowContentArr[2];
		String vacSpProductId =  vacRowContentArr[5];
		String key = phoneNumber + "#" + vacSpProductId;
		return key;
	}
	
	
	private List<String> getFileList(File file){
		List<String> fileContents = null;
		try {
			long start = System.currentTimeMillis();
			fileContents = FileUtils.readLines(file, CHARSET);
			long end = System.currentTimeMillis();
			System.out.println("get file content list takes time :" + (end - start));
		}catch(Exception e){
			fileContents = Collections.EMPTY_LIST;
			e.printStackTrace();
		}
		return fileContents;
	}
	
	/**
	 * 获取初始文件列表
	 */
	@SuppressWarnings("unchecked")
	public  List<File> getVACFileList(){
		List<File> listFiles = null;
		try {
//			File file = new File(VAC_RESOURCE_FOLDER_PATH);
			File file = new File(currentPath);
			listFiles = (List<File>) FileUtils.listFiles(file, FileFilterUtils.prefixFileFilter("SubscribeInfo"), null);
			for (File f : listFiles)  System.out.println("getVACFile====="+f.getName());
		} catch (Exception e) {
			e.printStackTrace();
		}
        
		return listFiles == null ? Collections.EMPTY_LIST : listFiles ;
	}
	
	/**
	 * 获取一个VAC文件的所有数据（缓存）
	 * @return
	 */
	public static String getVACDataString(VacFile vacFile) {
		List<String> dataList = vacFile.getDataListString();
		if(dataList==null) return "";
		StringBuffer sb = new StringBuffer();
		sb.append(vacFile.getFirstRow()).append(System.lineSeparator());
		dataList.forEach(recordString -> {
			sb.append(recordString)
			  .append(System.lineSeparator());
		});
		return sb.toString();
	}
	
	/**
	 * 打印
	 * @param obj
	 */
	public static void print(Object obj) {
		System.out.println(obj);
	}
	
	
	
	
	
	/**
	 * 静态内部类：专门生成VAC文件的线程
	 * 
	 * @author linzt
	 * @time 2018年3月1日 下午3:03:09
	 */
	static class VACFileDeletetor implements Runnable{
		
		private File delVacFile;
		
		public VACFileDeletetor(File delVacFile) {
			super();
			this.delVacFile = delVacFile;
		}

		@Override
		public void run(){
			try {
				long start = System.currentTimeMillis();
				
				FileUtils.deleteQuietly(delVacFile);
				
				long end = System.currentTimeMillis();
				System.out.println(" delete VAC files , takes time :"+(end-start) + ", fileName : " + delVacFile.getName());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		
	}
	
	/**
	 * 静态内部类：专门生成VAC文件的线程
	 * 
	 * @author linzt
	 * @time 2018年3月1日 下午3:03:09
	 */
	static class VACFileCreator implements Runnable{
		
		private VacFile vacFile ;
		
		public VACFileCreator(VacFile vacFile) {
			super();
			this.vacFile = vacFile;
		}

		@Override
		public void run(){
			try {
				//生成Vac文件到指定目录下
				long start = System.currentTimeMillis();
				File file = new File(vacFile.getFileName());
				String data = getVACDataString(vacFile);
				FileUtils.write(file, data, Charset.defaultCharset());
				long end = System.currentTimeMillis();
				System.out.println(" create VAC files , takes time :"+(end-start));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public VacFile getVacFile() {
			return vacFile;
		}

		public void setVacFile(VacFile vacFile) {
			this.vacFile = vacFile;
		}
	}

}
