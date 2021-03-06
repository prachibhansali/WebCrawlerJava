package com.ir.webcrawler;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class Frontier {
	HashMap<String,Tuple> frontier;
	int counter;
	int MAXDOC = 20000;
	HashMap<String,Long> domainStart = new HashMap<String,Long>();
	HashMap<String,Integer> countOccurences = new HashMap<String,Integer>();
	
	public Frontier()
	{
		frontier = new HashMap<String,Tuple>();
		counter=1;
	}

	public void push(ArrayList<String> topicwords,String url,int c) throws MalformedURLException
	{
		if(frontier.size() > MAXDOC) {
			removeMinScore(topicwords);
		}
		frontier.put(url,new Tuple(url,c,counter++));
	}

	public void push(ArrayList<String> topicwords,String url,int c,int priority) throws MalformedURLException
	{
		if(frontier.size() > MAXDOC) {
			removeMinScore(topicwords);
		}
		frontier.put(url,new Tuple(url,c,priority));
	}

	private void removeMinScore(ArrayList<String> topicwords) throws MalformedURLException {
		int NUM = 3000;
		Iterator<String> keys = frontier.keySet().iterator();
		double min=Integer.MAX_VALUE;
		HashMap<Integer,ArrayList<String>> res = new HashMap<Integer,ArrayList<String>>();
		while(keys.hasNext()){
			String key = (String) keys.next();
			int score = (int) ((frontier.get(key).getInlink()*
					getWordsFreq(key,topicwords))/(1+calcOccurenceAvg(new Url(key).getHost())));
			ArrayList<String> a = res.containsKey(score) ? res.get(score) : new ArrayList<String>();
			a.add(key);
			res.put(score, a);
		}
		ArrayList<Integer> sc = new ArrayList<Integer>(res.keySet());
		Collections.sort(sc);
		int count=0;
		int j=0;
		//System.out.println(res.size()+" "+res.get(sc.get(0)).size());
		while(count<=NUM && j<res.size()){
			for(int i=0;i<res.get(sc.get(j)).size() && count<=NUM;i++)
				{
					frontier.remove(res.get(sc.get(j)).get(i));
					count++;
				}
			j++;
		}
	}

	
	public void add(ArrayList<Tuple> temp)
	{
		for(int i=0;i<temp.size();i++)
			frontier.put(temp.get(i).getUrl(),temp.get(i));
	}

	public boolean containsUrl(String url)
	{
		Set<String> urls = new HashSet<String>(frontier.keySet());
		return urls.contains(url);
	}

	public Tuple pop(ArrayList<String> topicwords) throws MalformedURLException
	{
		String res =null;
		if(frontier.size()==0) return null;
		//HashMap<String,Double> scores = new HashMap<String,Double>();
		Iterator<String> keys = frontier.keySet().iterator();
		//int slotsize = frontier.size()/10;
		double maxscore=0.0;
		while(keys.hasNext()){
			String key = (String) keys.next();

			int score = (int) (frontier.get(key).getInlink()==Integer.MAX_VALUE ? Integer.MAX_VALUE : 
				(frontier.get(key).getInlink()*
						getWordsFreq(key,topicwords))/(1+calcOccurenceAvg(new Url(key).getHost())));
			//System.out.println("key "+key+" "+score+"\n");
			if(score >= maxscore)
			{
				if(score==maxscore)
				{
					if(res==null || (res!=null && frontier.get(key).getPriority() < frontier.get(res).getPriority()))
						res=key;
				}
				else res=key;
				maxscore = score;
			}
		}

		if(res==null)
		{
			int maxinlinks = findMaximumInlinkCount();
			int minpriority = Integer.MAX_VALUE;

			keys = frontier.keySet().iterator();
			while(keys.hasNext()){
				String key = (String) keys.next();
				if(frontier.get(key).getInlink() == maxinlinks)
					if(frontier.get(key).getPriority() < minpriority)
					{
						minpriority = frontier.get(key).getPriority();
						res = key;
					}
			}
		}
		Tuple t = frontier.get(res);
		frontier.remove(res);
		String host ="";
		try {
			host = new Url(res).getHost();
			if(!domainStart.containsKey(host)) {
				domainStart.put(host, System.currentTimeMillis());
				countOccurences.put(host, 1);
				System.out.println("Added start time for "+host);
			}
			else countOccurences.put(host, countOccurences.get(host)+1);
		} catch (MalformedURLException e) {
			System.out.println("Start time not recorded for "+host);
		}
		return t;
	}

	private double calcOccurenceAvg(String host) {
		if(!domainStart.containsKey(host)) return 0;
		long start = domainStart.get(host);
		long timeelapsed = (System.currentTimeMillis()-start)/1000;
		int count = countOccurences.get(host);
		
		return count/(double)timeelapsed;
	}

	private int getWordsFreq(String key, ArrayList<String> topicwords) {
		int count=0;
		for(int i=0;i<topicwords.size();i++)
			if(key.contains(topicwords.get(i)))
				count++;
		return count;
	}

	/*public Tuple pop()
	{
		Tuple url = null;
		if(frontier.size()==0) return null;
		int maxinlinks = findMaximumInlinkCount();
		int minpriority = Integer.MAX_VALUE;

		Iterator<String> keys = frontier.keySet().iterator();
		while(keys.hasNext()){
			String key = (String) keys.next();
			if(frontier.get(key).getInlink() == maxinlinks)
				if(frontier.get(key).getPriority() < minpriority)
				{
					minpriority = frontier.get(key).getPriority();
					url = frontier.get(key);
				}
		}
		frontier.remove(url.getUrl());
		//System.out.println(urls.indexOf("https://wikimediafoundation.org/wiki/Privacy_policy"));
		return url;
	}*/

	private int findMaximumInlinkCount() {
		Iterator<String> keys = frontier.keySet().iterator();
		ArrayList<Integer> inlinks = new ArrayList<Integer>();
		while(keys.hasNext()){
			String key = (String) keys.next();
			inlinks.add(frontier.get(key).getInlink());
		}
		return Collections.max(inlinks);
	}

	public void incrementInlink(String url,int n)
	{
		int newcount = (frontier.get(url).getInlink()+n);
		frontier.put(url,new Tuple(url,newcount,frontier.get(url).getPriority()));
	}

	public int getInlinks(String url)
	{
		return frontier.get(url).getInlink();
	}

	public int size()
	{
		return frontier.size();
	}

	public void removeFromFrontier(String domain) {
		Iterator<String> i = new HashMap<String,Tuple>(frontier).keySet().iterator();
		while(i.hasNext()){
			String key = (String) i.next();
			if(key.contains(domain))
				frontier.remove(key);
		}
	}
}
