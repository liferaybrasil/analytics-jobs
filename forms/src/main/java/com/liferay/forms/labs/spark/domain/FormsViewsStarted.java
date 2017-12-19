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

import java.sql.Timestamp;

/**
 * @author Leonardo Barros
 */
public class FormsViewsStarted {

	public FormsViewsStarted(
		long userId, long formId, String event, Timestamp date,
		String analyticsKey) {

		this.userId = userId;
		this.formId = formId;
		this.event = event;
		this.date = date;
		this.analyticsKey = analyticsKey;
	}

	public String getAnalyticsKey() {

		return analyticsKey;
	}

	public Timestamp getDate() {

		return date;
	}

	public String getEvent() {

		return event;
	}

	public long getFormId() {

		return formId;
	}

	public long getUserId() {

		return userId;
	}

	public void setAnalyticsKey(String analyticsKey) {

		this.analyticsKey = analyticsKey;
	}

	public void setDate(Timestamp date) {

		this.date = date;
	}

	public void setEvent(String event) {

		this.event = event;
	}

	public void setFormId(long formId) {

		this.formId = formId;
	}

	public void setUserId(long userId) {

		this.userId = userId;
	}

	private String analyticsKey;
	private Timestamp date;
	private String event;
	private long formId;
	private long userId;
}
