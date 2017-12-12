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

package com.liferay.forms.labs.spark;

import java.sql.Date;

/**
 * @author Leonardo Barros
 */
public class FormFieldsAggregatedData {

	public String getAnalyticskey() {
		return analyticskey;
	}

	public void setAnalyticskey(String analyticskey) {
		this.analyticskey = analyticskey;
	}

	public long getFormid() {
		return formid;
	}

	public void setFormid(long formid) {
		this.formid = formid;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public long getInteractions() {
		return interactions;
	}

	public void setInteractions(long interactions) {
		this.interactions = interactions;
	}

	public long getTotaltime() {
		return totaltime;
	}

	public void setTotaltime(long totaltime) {
		this.totaltime = totaltime;
	}

	public long getEmpty() {
		return empty;
	}

	public void setEmpty(long empty) {
		this.empty = empty;
	}

	public long getRefilled() {
		return refilled;
	}

	public void setRefilled(long refilled) {
		this.refilled = refilled;
	}

	public long getDropoffs() {
		return dropoffs;
	}

	public void setDropoffs(long dropoffs) {
		this.dropoffs = dropoffs;
	}

	private String analyticskey;
	private long formid;
	private Date date;
	private String field;
	private long interactions;
	private long totaltime;
	private long empty;
	private long refilled;
	private long dropoffs;
}