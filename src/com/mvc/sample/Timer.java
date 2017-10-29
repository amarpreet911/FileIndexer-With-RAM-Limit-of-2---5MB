package com.mvc.sample;

public class Timer {

	private long startTime;
	private long estimatedTime;
	
	public long startTimer(){
		startTime = System.currentTimeMillis();
		return startTime;
	}
	
	public long stopTimer(){
		estimatedTime = System.currentTimeMillis() - startTime;
		return estimatedTime;
	}
	
	public String getTimeElapsed(long milliseconds){
		String elapsed;
		
		int remainingMillis = (int) (milliseconds) % 60 ;
		int seconds = (int) (milliseconds / 1000) % 60 ;
		int minutes = (int) ((milliseconds / (1000*60)) % 60);
//		int hours   = (int) ((milliseconds / (1000*60*60)) % 24);
	
//		elapsed = hours + "h : " + minutes + "m : " + seconds + "s : " + milliseconds + "ms";
//		elapsed = minutes + "m : " + seconds + "s : " + milliseconds + "ms";
		elapsed = minutes + "m : " + seconds + "s : " + remainingMillis + "ms";
		return elapsed;
	}

}
