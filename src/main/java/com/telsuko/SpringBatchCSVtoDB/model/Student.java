package com.telsuko.SpringBatchCSVtoDB.model;

public class Student
{
	private int id;
	private String name;
	private int m1;
	private int m2;
	
	public static String[] fields()
	{
		return new String[] {"id","name","m1","m2"};
	}
	
	public Student()
	{
		
	}
	
	public Student(int id, String name, int m1, int m2) {
		super();
		this.id = id;
		this.name = name;
		this.m1 = m1;
		this.m2 = m2;
	}
	
	public int getId() { return id;}
	public void setId(int id) { this.id = id;}
	
	public String getName() { return name;}
	public void setName(String name) { this.name = name;}
	
	public int getM1() { return m1;}
	public void setM1(int m1) {	this.m1 = m1;}
	
	public int getM2() {return m2;}
	public void setM2(int m2) {	this.m2 = m2;}

}
