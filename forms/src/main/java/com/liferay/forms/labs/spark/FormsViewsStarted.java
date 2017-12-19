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

	public long getUserId() {

		return userId;
	}

	public void setUserId(long userId) {

		this.userId = userId;
	}

	public long getFormId() {

		return formId;
	}

	public void setFormId(long formId) {

		this.formId = formId;
	}

	public String getEvent() {

		return event;
	}

	public void setEvent(String event) {

		this.event = event;
	}

	public Timestamp getDate() {

		return date;
	}

	public void setDate(Timestamp date) {

		this.date = date;
	}

	public String getAnalyticsKey() {

		return analyticsKey;
	}

	public void setAnalyticsKey(String analyticsKey) {

		this.analyticsKey = analyticsKey;
	}

	private long userId;
	private long formId;
	private String event;
	private Timestamp date;
	private String analyticsKey;
}
