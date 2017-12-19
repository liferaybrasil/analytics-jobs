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
public class FormsAggregatedData {

	public FormsAggregatedData(String analyticskey, long formId, Date date) {
		this.analyticskey = analyticskey;
		this.formid = formId;
		this.date = date;
	}

	public String getAnalyticskey() {
		return analyticskey;
	}

	public long getConverted() {
		return converted;
	}

	public long getConvertedtotaltime() {
		return convertedtotaltime;
	}

	public Date getDate() {
		return date;
	}

	public long getDropoffs() {
		return dropoffs;
	}

	public long getFormid() {
		return formid;
	}

	public long getSessions() {
		return sessions;
	}

	public long getStarted() {
		return started;
	}

	public long getViews() {
		return views;
	}

	public void setAnalyticskey(String analyticskey) {
		this.analyticskey = analyticskey;
	}

	public void setConverted(long converted) {
		this.converted = converted;
	}

	public void setConvertedtotaltime(long convertedtotaltime) {
		this.convertedtotaltime = convertedtotaltime;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public void setDropoffs(long dropoffs) {
		this.dropoffs = dropoffs;
	}

	public void setFormid(long formid) {
		this.formid = formid;
	}

	public void setSessions(long sessions) {
		this.sessions = sessions;
	}

	public void setStarted(long started) {
		this.started = started;
	}

	public void setViews(long views) {
		this.views = views;
	}

	private String analyticskey;
	private long converted;
	private long convertedtotaltime;
	private Date date;
	private long dropoffs;
	private long formid;
	private long sessions;
	private long started;
	private long views;
}
