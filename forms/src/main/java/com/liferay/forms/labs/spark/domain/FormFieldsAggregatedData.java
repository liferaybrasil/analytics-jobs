/**
 * Copyright (c) 2000-present Liferay, Inc. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 */

package com.liferay.forms.labs.spark.domain;

import java.sql.Date;

/**
 * @author Leonardo Barros
 */
public class FormFieldsAggregatedData {

	public String getAnalyticskey() {

		return analyticskey;
	}

	public Date getDate() {

		return date;
	}

	public long getDropoffs() {

		return dropoffs;
	}

	public long getEmpty() {

		return empty;
	}

	public String getField() {

		return field;
	}

	public long getFormid() {

		return formid;
	}

	public long getInteractions() {

		return interactions;
	}

	public long getRefilled() {

		return refilled;
	}

	public long getTotaltime() {

		return totaltime;
	}

	public void setAnalyticskey(String analyticskey) {

		this.analyticskey = analyticskey;
	}

	public void setDate(Date date) {

		this.date = date;
	}

	public void setDropoffs(long dropoffs) {

		this.dropoffs = dropoffs;
	}

	public void setEmpty(long empty) {

		this.empty = empty;
	}

	public void setField(String field) {

		this.field = field;
	}

	public void setFormid(long formid) {

		this.formid = formid;
	}

	public void setInteractions(long interactions) {

		this.interactions = interactions;
	}

	public void setRefilled(long refilled) {

		this.refilled = refilled;
	}

	public void setTotaltime(long totaltime) {

		this.totaltime = totaltime;
	}

	private String analyticskey;
	private Date date;
	private long dropoffs;
	private long empty;
	private String field;
	private long formid;
	private long interactions;
	private long refilled;
	private long totaltime;
}
