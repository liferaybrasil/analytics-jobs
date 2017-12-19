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
public class FormsAggregatedData {

	public FormsAggregatedData(String analyticskey, long formId, Date date) {
		this.analyticskey = analyticskey;
		this.formid = formId;
		this.date = date;
	}

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

	public long getViews() {
		return views;
	}

	public void setViews(long views) {
		this.views = views;
	}

	public long getSessions() {
		return sessions;
	}

	public void setSessions(long sessions) {
		this.sessions = sessions;
	}

	public long getStarted() {
		return started;
	}

	public void setStarted(long started) {
		this.started = started;
	}

	public long getConverted() {
		return converted;
	}

	public void setConverted(long converted) {
		this.converted = converted;
	}

	public long getConvertedtotaltime() {
		return convertedtotaltime;
	}

	public void setConvertedtotaltime(long convertedtotaltime) {
		this.convertedtotaltime = convertedtotaltime;
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
	private long views;
	private long sessions;
	private long started;
	private long converted;
	private long convertedtotaltime;
	private long dropoffs;
}
