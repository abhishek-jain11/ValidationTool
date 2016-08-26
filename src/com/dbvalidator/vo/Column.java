
package com.dbvalidator.vo;

/**
 *
 * @author AJ
 */
public class Column {

	String name;
	String dataType;
	Verify verify;
	String validator;
	String tableName;
	Integer datalength;
	Integer dataprecision;
	Integer datascale;

	public Column(final String name, final String dataType, final String tableName, final Integer dataLength, final Integer dataPresicison,
			final Integer dataScale) {
		this.name = name;
		this.dataType = dataType;
		this.tableName = tableName;
		this.datalength = dataLength;
		this.dataprecision = dataPresicison;
		this.datascale = dataScale;
	}

	public Integer getDatalength() {
		return datalength;
	}

	public void setDatalength(final Integer datalength) {
		this.datalength = datalength;
	}

	public Integer getDataprecision() {
		return dataprecision;
	}

	public void setDataprecision(final Integer dataprecision) {
		this.dataprecision = dataprecision;
	}

	public Integer getDatascale() {
		return datascale;
	}

	public void setDatascale(final Integer datascale) {
		this.datascale = datascale;
	}

	public Column() {
		super();
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(final String tableName) {
		this.tableName = tableName;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(final String dataType) {
		this.dataType = dataType;
	}

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public String getValidator() {
		return validator;
	}

	public Verify getVerify() {
		return verify;
	}

	public void setVerify(final String verify) {
		this.verify = Verify.valueOf(verify);
	}

	public void setValidator(final String validator) {
		this.validator = validator;
	}

	public void override(final Column col) {
		if (col.verify != null) {
			this.verify = col.verify;
		}

		if (col.validator != null) {
			this.setValidator(col.getValidator());
		}
	}

}
