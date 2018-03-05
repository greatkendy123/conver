package com.unicom.model;

import java.util.List;

public class VacFile {

	private String fileName;
	private String firstRow;
	private List<Vac> dataList;
	
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

	
}
