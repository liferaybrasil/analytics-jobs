package com.liferay.forms.labs.spark;

import java.sql.Timestamp;

public class ViewsStartedBean {

	public ViewsStartedBean(long userId, long formId, String event, Timestamp date, String analyticsKey) {
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
