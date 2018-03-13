package com.unicom.model;

import java.util.List;

public class VacFile {

	private String fileName;
	private String firstRow;
	private List<Vac> dataList;
	private List<String> dataListString;
	
	public VacFile() {
		super();
	}

	/**
	 * @param fileName
	 * @param firstRow
	 * @param dataList
	 */
	public VacFile(String fileName, String firstRow, List<Vac> dataList) {
		super();
		this.fileName = fileName;
		this.firstRow = firstRow;
		this.dataList = dataList;
	}
	
	


	public VacFile(String fileName, String firstRow) {
		super();
		this.fileName = fileName;
		this.firstRow = firstRow;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFirstRow() {
		return firstRow;
	}

	public void setFirstRow(String firstRow) {
		this.firstRow = firstRow;
	}

	public List<Vac> getDataList() {
		return dataList;
	}

	public void setDataList(List<Vac> dataList) {
		this.dataList = dataList;
	}

	public List<String> getDataListString() {
		return dataListString;
	}

	public void setDataListString(List<String> dataListString) {
		this.dataListString = dataListString;
	}

	
	
}
