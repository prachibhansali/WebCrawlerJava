package com.ir.webcrawler;

public class Tuple {
	String url;
	int inlink;
	int priority;
	
	public Tuple(String url, int inlink, int p) {
		setUrl(url);
		setInlink(inlink);
		setPriority(p);
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public int getInlink() {
		return inlink;
	}
	public void setInlink(int inlink) {
		this.inlink = inlink;
	}
	public int getPriority() {
		return priority;
	}
	public void setPriority(int priority) {
		this.priority = priority;
	}
	
	public String toString()
	{
		return url+" "+inlink+" "+priority;
	}
}
