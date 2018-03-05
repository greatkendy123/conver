package com.unicom.model;

import java.util.List;

public class CBSSFile {

	private String fileName;
	private String firstRow;
	private List<String> dataList;
	
	public CBSSFile() {
		super();
	}

	/**
	 * @param fileName
	 * @param firstRow
	 * @param dataList
	 */
	public CBSSFile(String fileName, String firstRow, List<String> dataList) {
		super();
		this.fileName = fileName;
		this.firstRow = firstRow;
		this.dataList = dataList;
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

	public List<String> getDataList() {
		return dataList;
	}

	public void setDataList(List<String> dataList) {
		this.dataList = dataList;
	}

	
}
