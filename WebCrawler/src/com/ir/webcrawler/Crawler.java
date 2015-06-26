package com.ir.webcrawler;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Entities.EscapeMode;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Whitelist;
import org.jsoup.select.Elements;
public class Crawler {
	public static void main(String args[]) throws Exception
	{
		HashMap<Integer,Boolean> defaultPorts;
		HashMap<String,Boolean> discardHosts;

		Crawler cw = new Crawler();

		defaultPorts = new HashMap<Integer,Boolean>();
		defaultPorts.put(80,true);
		defaultPorts.put(433,true);

		discardHosts = new HashMap<String,Boolean>();
		discardHosts.put("twitter.com",true);
		discardHosts.put("facebook.com",true);
		discardHosts.put("github.com",true);
		discardHosts.put("www.mediawiki.org",true);
		discardHosts.put("meta.wikimedia.org",true);
		discardHosts.put("donate.wikimedia.org",true);
		discardHosts.put("www.wikidata.org",true);
		discardHosts.put("commons.wikimedia.org",true);


		Frontier f = new Frontier();
		cw.addSeedsToFrontier(f,defaultPorts,discardHosts);
		/*Tuple t = f.pop();
		System.out.println(t.getUrl()+" "+ t.getPriority()+" "+t.getInlink());
		t = f.pop();
		System.out.println(t.getUrl()+" "+ t.getPriority()+" "+t.getInlink());
		t = f.pop();
		System.out.println(t.getUrl()+" "+ t.getPriority()+" "+t.getInlink());*/
		
		cw.parse(f,defaultPorts,discardHosts);
	}

	private void parse(Frontier f, HashMap<Integer, Boolean> defaultPorts, HashMap<String, Boolean> discardHosts) throws Exception {
		int parsedCount = 0;
		HashMap<String,Long> domainTimes = new HashMap<String,Long>();
		HashMap<String,Integer> processedUrls = new HashMap<String,Integer>();
		while(f.size() > 0 && parsedCount<20000){
			manageURLS(f,domainTimes,processedUrls,defaultPorts,discardHosts);
		}
	}

	private void manageURLS(Frontier f, HashMap<String, Long> domainTimes, HashMap<String, Integer> processedUrls, HashMap<Integer, Boolean> defaultPorts, HashMap<String, Boolean> discardHosts) throws MalformedURLException, InterruptedException {
		Tuple t = f.pop();
		Frontier temp = new Frontier();
		
		while(!isValidDomain(domainTimes,new Url(t.getUrl()).getHost()) && f.size() > 0)
		{
			temp.push(t.getUrl(), t.getInlink(),t.getPriority());
			//System.out.println("Added to temp "+t.toString());
			t = f.pop();
		}
		if(isValidDomain(domainTimes,new Url(t.getUrl()).getHost())) {
			domainTimes.put(new Url(t.getUrl()).getHost(), System.currentTimeMillis());
			f.add(temp);
			processUrl(t,processedUrls,f,defaultPorts,discardHosts);
			processedUrls.put(t.getUrl(),t.getInlink());
			domainTimes.put(new Url(t.getUrl()).getHost(), System.currentTimeMillis());
		}
		else {
			Thread.sleep(1000l);
			temp.push(t.getUrl(), t.getInlink(),t.getPriority());
			f.add(temp);
		}
		removeAllowableDomains(domainTimes);
	}

	private void processUrl(Tuple t, HashMap<String, Integer> processedUrls, Frontier f, HashMap<Integer, Boolean> defaultPorts, HashMap<String, Boolean> discardHosts) throws InterruptedException {
		String url = t.getUrl();
		if(canBeRobotParsed(url))
		{
			Thread.sleep(1000); // add crawl delay if present
			try{
				extractDocumentInfo(url,processedUrls,f,defaultPorts,discardHosts);
			}
			catch(IOException e) {
				System.out.println("url could not be parsed "+url+" "+e.toString());
			}
			catch(JSONException e) {
				System.out.println("JSON could not be parsed "+e.toString());
			}
		}
		return;
	}
	
	/*
	 * String url = t.getUrl();
		if(processedUrls.containsKey(url)) 
			processedUrls.put(url, processedUrls.get(url)+1);
		else 
		{
			if(f.containsUrl(url))
				f.incrementInlink(url, 1);
			
		}
	 */

	private boolean canBeRobotParsed(String url) {
		
		return false;
	}

	private void extractDocumentInfo(String url,
			HashMap<String, Integer> processedUrls, Frontier f, 
			HashMap<Integer, Boolean> defaultPorts, 
			HashMap<String, Boolean> discardHosts) throws IOException,JSONException {
		
		JSONObject obj = fetchInfoAsJson(url);
		ArrayList<String> outlinks = addOutlinksToFrontier(obj.getJSONArray("links"),f,processedUrls,defaultPorts,discardHosts);
		writeToLinkGraph(url,outlinks);
		writeToAP89(obj.getString("id"),obj.getString("title"),obj.getString("text"));
	}

	private ArrayList<String> addOutlinksToFrontier(JSONArray jsonArray, Frontier f,
			HashMap<String, Integer> processedUrls, HashMap<Integer, Boolean> defaultPorts, 
			HashMap<String, Boolean> discardHosts) throws JSONException {
		ArrayList<String> outlinks = new ArrayList<String>();
		for(int i=0;i<jsonArray.length();i++)
		{
			String url = jsonArray.getString(i);
			url = canonicalizeURL(url,defaultPorts,discardHosts);
			if(url==null) continue;
			if(processedUrls.containsKey(url)) 
				processedUrls.put(url, processedUrls.get(url)+1);
			else 
			{
				if(f.containsUrl(url))
					f.incrementInlink(url, 1);
				else {
					outlinks.add(url);
					f.push(url, 1);
				}
			}
		}
		return outlinks;
	}

	private void writeToAP89(String id, String title, String text) throws IOException {
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("LinkGraph.txt", true)));
		out.println("<DOC>");
		out.println("<DOCNO> "+id+" </DOCNO>");
		out.println("<HEAD> "+title+" </HEAD>");
		out.println("<TEXT> "+text+" </TEXT>");
		out.println("</DOC>");
		out.close();
	}

	private JSONObject fetchInfoAsJson(String url) throws IOException,JSONException{
		JSONObject obj = new JSONObject();
		JSONArray jarr = new JSONArray();
		Document document = Jsoup.connect(url).get();
		obj.put("id",url);
		obj.put("header",document.head());
		obj.put("title",document.title());
		obj.put("rawHtml","raw");//document.body().html());
		
		Elements links = document.select("a[href]");
        for (Element link : links)
        	jarr.put(link.attr("abs:href"));//.put(obj.put("link",link.attr("abs:href")));
        //obj.put("links",jarr);
        obj.put("links",jarr);
        document = new Cleaner(Whitelist.simpleText()).clean(document);
	    
	    // Adjust escape mode
	    document.outputSettings().escapeMode(EscapeMode.xhtml);

	    // Get back the string of the body.
	    obj.put("text",document.html().replaceAll("\\\\n", "\n"));
	    return obj;
	}

	private void writeToLinkGraph(String url, ArrayList<String> outlinks) throws IOException,JSONException {
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("LinkGraph.txt", true)));
		out.print(url+"\t");
		for(int i=0;i<outlinks.size();i++)
			out.print(outlinks.get(i)+"\t");
		out.println();
		out.close();
	}

	private void removeAllowableDomains(HashMap<String, Long> domainTimes) {
		HashMap<String,Long> h = new HashMap<String,Long>(domainTimes);
		Iterator<String> keys = h.keySet().iterator();
		while(keys.hasNext())
		{
			String key = (String) keys.next();
			if((System.currentTimeMillis() - domainTimes.get(key)) > 1000)
			{
				System.out.println(key);
				domainTimes.remove(key);
			}
		}
	}

	private boolean isValidDomain(HashMap<String, Long> domainTimes,String domain) {
		return !(domainTimes.containsKey(domain));
	}

	private boolean isHTML(String url) throws Exception
	{
		HttpURLConnection urlc = (HttpURLConnection) new URL(url).openConnection();
		urlc.setAllowUserInteraction( false );
		urlc.setDoInput( true );
		urlc.setDoOutput( false );
		urlc.setUseCaches( true );
		urlc.setRequestMethod("HEAD");
		urlc.connect();
		String mime = urlc.getContentType();
		if(mime.equals("text/html")){
			return true;
		}
		return false;
	}

	private void addSeedsToFrontier
	(Frontier f, HashMap<Integer, Boolean> defaultPorts, HashMap<String, Boolean> discardHosts) throws IOException {
		// Reading file for seed URLS
		BufferedReader br = new BufferedReader(new FileReader("seedurls"));
		String url="";
		while((url=br.readLine())!=null)
		{
			String curl = canonicalizeURL(url,defaultPorts,discardHosts);
			if(curl!=null) f.push(curl, Integer.MAX_VALUE);
		}
		br.close();
	}

	private String canonicalizeURL(String url,HashMap<Integer,Boolean> defaultPorts,HashMap<String, Boolean> discardHosts){
		url = url.trim();
		URL u;
		try {
			u = new URL(url);
		}
		catch(Exception e) {
			return null;
		}

		String protocol = u.getProtocol();
		String host = u.getHost();
		if(discardHosts.containsKey(host)) return null;
		String query = u.getQuery();
		String path = u.getPath();
		String filename = u.getFile();
		int port = u.getPort();
		String portstr ="";
		if(protocol == "" && url.startsWith("//"))
			protocol = "http";

		if(query !="") 
			query = "?"+query;

		if(!defaultPorts.containsKey(port) && port!=-1)
			portstr = ":"+port;

		return protocol.toLowerCase()+"://"+host.toLowerCase()+portstr+filename;
	}
}
