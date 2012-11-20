package com.dp.nebula.wormhole.plugins.common;

import java.util.List;

/**
 * MetaData records a source or sink Database meta information.
 * 
 * */
public class MetaData {
	private String dataBaseName;

	private String dataBaseVersion;

	private String tableName = "default_table";
	
	private List<Column> colInfo;
	
	/**
	 * Get name of this table.
	 * 
	 * @return
	 * 			name of table.
	 * 
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * Set name of this table.
	 * 
	 * @param	tableName
	 * 			name of this table.
	 * 
	 * */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * Get name of database.
	 * 
	 * @return
	 * 			name of database.
	 * 
	 * */
	public String getDataBaseName() {
		return dataBaseName;
	}

	/**
	 * Set name of database.
	 * 
	 * @param	dataBaseName
	 * 			name of database.
	 * 
	 * */
	public void setDataBaseName(String dataBaseName) {
		this.dataBaseName = dataBaseName;
	}

	/**
	 * Get version of database.
	 * 
	 * @return		
	 * 			version of database.
	 * */
	public String getDataBaseVersion() {
		return this.dataBaseVersion;
	}

	/**
	 * Set version of database.
	 * 
	 * @param	dataBaseVersion
	 * 			version of database.
	 * 
	 * */
	public void setDataBaseVersion(String dataBaseVersion) {
		this.dataBaseVersion = dataBaseVersion;
	}

	/**
	 * Get information of all columns.
	 * 
	 * @return		
	 * 			columns information.
	 * 
	 * */
	public List<Column> getColInfo() {
		return colInfo;
	}

	/**
	 * Set column information.
	 * 
	 * @param	colInfo
	 * 			a list of column information.
	 * 
	 * */
	public void setColInfo(List<Column> colInfo) {
		this.colInfo = colInfo;
	}

	public class Column {
		private boolean isText = false;

		private boolean isNum = false;

		private String colName;

		private String dataType; // no use now

		private boolean isPK;

		public String getDataType() {
			return dataType;
		}

		public String getColName() {
			return colName;
		}

		public void setDataType(String dataType) {
			this.dataType = dataType;
		}

		public boolean isPK() {
			return isPK;
		}

		public void setPK(boolean isPK) {
			this.isPK = isPK;
		}

		public boolean isText() {
			return isText;
		}

		public void setText(boolean isText) {
			this.isText = isText;
		}

		public boolean isNum() {
			return isNum;
		}

		public void setNum(boolean isNum) {
			this.isNum = isNum;
		}

		public void setColName(String name) {
			colName = name;
		}
	}
}
