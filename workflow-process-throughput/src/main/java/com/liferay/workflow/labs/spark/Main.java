package com.liferay.workflow.labs.spark;

import java.util.Timer;

import org.apache.spark.sql.AnalysisException;

public class Main {

    public static void main(String[] args) throws AnalysisException {
    	Timer timer = new Timer();

    	timer.schedule(new Job(), 0, 1000 * 60 * 5);
    }
}