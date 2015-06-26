package com.ir.webcrawler;

import java.util.ArrayList;
import java.util.Collections;

public class Frontier {
	ArrayList<String> urls;
	ArrayList<Integer> inlinks;
	ArrayList<Integer> priorities;
	int counter;
	
	public Frontier()
	{
		urls = new ArrayList<String>();
		inlinks = new ArrayList<Integer>();
		priorities = new ArrayList<Integer>();
		counter=1;
	}
	
	public void push(String url,int c)
	{
		if(urls.size() > 20000) return;
		urls.add(url);
		inlinks.add(c);
		priorities.add(counter++);
	}
	
	public void push(String url,int c,int priority)
	{
		if(urls.size() > 20000) return;
		urls.add(url);
		inlinks.add(c);
		priorities.add(priority);
	}
	
	public void add(Frontier temp)
	{
		urls.addAll(temp.urls);
		inlinks.addAll(temp.inlinks);
		priorities.addAll(temp.priorities);
	}
	
	public boolean containsUrl(String url)
	{
		return urls.contains(url);
	}
	
	public Tuple pop()
	{
		String url = null;
		int index = -1;
		if(urls.size()==0) return null;
		int maxinlinks = Collections.max(inlinks);
		int minpriority = Integer.MAX_VALUE;
		for(int i=0;i<urls.size();i++)
		{
			if(inlinks.get(i)==maxinlinks)
				if(priorities.get(i) < minpriority)
				{
					minpriority = priorities.get(i);
					url = urls.get(i);
					index = i;
				}
		}
		urls.remove(index);
		inlinks.remove(index);
		priorities.remove(index);
		
		return new Tuple(url,maxinlinks,minpriority);
	}
	
	public void incrementInlink(String url,int n)
	{
		int index = urls.indexOf(url);
		if(index!=-1)
			inlinks.set(index, inlinks.get(index)+n);
	}
	
	public int getInlinks(String url)
	{
		return inlinks.get(urls.indexOf(url));
	}
	
	public int size()
	{
		return urls.size();
	}
}
