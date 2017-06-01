package org.apache.hadoop.score;

public class Info {
	private String name;
	private double score;
	public Info(String name, double score){
		this.name = name;
		this.score = score;
	}
	public double getScore(){
		return score;
	}
	public String getName(){
		return name;
	}
}
