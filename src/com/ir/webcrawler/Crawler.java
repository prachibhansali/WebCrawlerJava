package com.ir.webcrawler; 
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
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

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRules.RobotRulesMode;
import crawlercommons.robots.SimpleRobotRulesParser;
public class Crawler {
	private HashMap<String,Integer> docs ;
	public static void main(String args[]) throws Exception
	{
		/*Crawler cw = new Crawler();
		HashMap<String, Boolean> spiderTrapDomains;
		spiderTrapDomains = new HashMap<String,Boolean>();
		cw.docs = new HashMap<String,Integer>();
		JSONObject obj = cw.fetchInfoAsJson("http://www.ibtimes.co.uk/italy-over-2500-migrants-rescued-mediterranean-day-1507605", spiderTrapDomains);
		//System.out.println(obj.toString());
		System.out.println(cw.sendToIndex(obj));*/
		
		PrintWriter out = new PrintWriter("LinkGraph.txt");
		PrintWriter pw = new PrintWriter("AP89.txt");
		out.close();
		pw.close();
		
		HashMap<Integer,Boolean> defaultPorts;
		HashMap<String,Boolean> discardHosts;
		HashMap<String, Boolean> spiderTrapDomains;

		Crawler cw = new Crawler();

		cw.docs = new HashMap<String,Integer>();
		
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
		discardHosts.put("shop.wikimedia.org",true);
		discardHosts.put("toolserver.org",true);
		discardHosts.put("wikimediafoundation.org",true);
		
		spiderTrapDomains = new HashMap<String,Boolean>();
		Frontier f = new Frontier();
		ArrayList<String> topicwords = new ArrayList<String>();
		readWordsFromFile(topicwords);
		cw.addSeedsToFrontier(topicwords,f,defaultPorts,discardHosts);
		/*Tuple t = f.pop();
		System.out.println(t.getUrl()+" "+ t.getPriority()+" "+t.getInlink());
		t = f.pop();
		System.out.println(t.getUrl()+" "+ t.getPriority()+" "+t.getInlink());
		t = f.pop();
		System.out.println(t.getUrl()+" "+ t.getPriority()+" "+t.getInlink());*/
		cw.parse(topicwords,f,defaultPorts,discardHosts,spiderTrapDomains);
		
	}

	private static void readWordsFromFile(ArrayList<String> topicwords) throws Exception{
		BufferedReader br = new BufferedReader(new FileReader("words"));
		String line="";
		while((line=br.readLine())!=null)
		{
			topicwords.add(line.trim());
		}
		br.close();
	}

	private void parse(ArrayList<String> topicwords,Frontier f, HashMap<Integer, Boolean> defaultPorts, HashMap<String, Boolean> discardHosts, HashMap<String, Boolean> spiderTrapDomains) throws Exception {
		HashMap<String,Long> domainTimes = new HashMap<String,Long>();
		HashMap<String,Integer> processedUrls = new HashMap<String,Integer>();
		while(f.size() > 0 && docs.size()<20000){
			System.out.println("Docs processed = "+docs.size()+" Frontier size = "+f.size());
			
			manageURLS(topicwords,f,domainTimes,processedUrls,defaultPorts,discardHosts,spiderTrapDomains);
		}
	}

	private void manageURLS(ArrayList<String> topicwords,Frontier f, HashMap<String, Long> domainTimes, HashMap<String, Integer> processedUrls, 
			HashMap<Integer, Boolean> defaultPorts, HashMap<String, Boolean> discardHosts, 
			HashMap<String, Boolean> spiderTrapDomains) throws InterruptedException, IOException {
		Tuple t = f.pop(topicwords);
		ArrayList<Tuple> temp = new ArrayList<Tuple>();
		
		while(!isValidDomain(domainTimes,new Url(t.getUrl()).getHost()) && f.size() > 0 
				&& !spiderTrapDomains.containsKey(extractQueryURL(t.getUrl())))
		{
			if(t.getInlink() == Integer.MAX_VALUE) {
				Thread.sleep(1000);
				removeAllowableDomains(domainTimes);
				continue;
			}
			temp.add(new Tuple(t.getUrl(), t.getInlink(),t.getPriority()));
			//System.out.println("Added to temp "+t.toString());
			t = f.pop(topicwords);
		}
		if(isValidDomain(domainTimes,new Url(t.getUrl()).getHost())) {
			domainTimes.put(new Url(t.getUrl()).getHost(), System.currentTimeMillis());
			f.add(temp);
			if(processUrl(topicwords,t,processedUrls,f,defaultPorts,discardHosts,spiderTrapDomains)){
			if(t.getInlink() >= Integer.MAX_VALUE)
				processedUrls.put(t.getUrl(),0);
			else processedUrls.put(t.getUrl(),t.getInlink());
			}
			domainTimes.put(new Url(t.getUrl()).getHost(), System.currentTimeMillis());
		}
		else {
			System.out.println("SLEEPING!");
			Thread.sleep(1000l);
			temp.add(new Tuple(t.getUrl(), t.getInlink(),t.getPriority()));
			f.add(temp);
		}
		removeAllowableDomains(domainTimes);
	}

	private boolean processUrl(ArrayList<String> topicwords, Tuple t, HashMap<String, Integer> processedUrls, Frontier f, 
			HashMap<Integer, Boolean> defaultPorts, HashMap<String, Boolean> discardHosts
			,HashMap<String, Boolean> spiderTrapDomains) throws InterruptedException, IOException {
		String url = t.getUrl();
		if(canBeRobotParsed(url))
		{
			System.out.println("checking robot "+url+" "+t.getInlink());
			Thread.sleep(1000); // add crawl delay if present
			try{
				return extractDocumentInfo(topicwords,url,processedUrls,f,defaultPorts,discardHosts,spiderTrapDomains);
			}
			catch(IOException e) {
				System.out.println("url could not be parsed "+url+" "+e.toString());
			}
			catch(JSONException e) {
				System.out.println("JSON could not be parsed "+e.toString());
			}
		}
		spiderTrapDomains.put(extractQueryURL(url), true);
		return false;
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

	private String extractQueryURL(String url) throws IOException{
		URL u = new URL(url);
		String domain = u.getHost()+(u.getPort()==-1? "":(":"+u.getPort()))+(u.getPath().equals("")?"/":u.getPath())+"?";
		return domain;
	}

	private boolean canBeRobotParsed(String url) throws IOException {
		String user_agent = "whatever";
		return isCrawlable(url,user_agent);
	}

	
	public static boolean isCrawlable(String page_url, String user_agent) {
		try {
			URL urlObj = new URL(page_url);
			String hostId = urlObj.getProtocol() + "://" + urlObj.getHost()
					+ (urlObj.getPort() > -1 ? ":" + urlObj.getPort() : "");
			//System.out.println(hostId);
			Map<String, BaseRobotRules> robotsTxtRules = new HashMap<String, BaseRobotRules>();
			BaseRobotRules rules = robotsTxtRules.get(hostId);
			if (rules == null) {
				String robotsContent = getContents(hostId + "/robots.txt",user_agent);
				if (robotsContent == null) {
					rules = new SimpleRobotRules(RobotRulesMode.ALLOW_ALL);
				} else {
					SimpleRobotRulesParser robotParser = new SimpleRobotRulesParser();
					rules = robotParser.parseContent(hostId,
							IOUtils.toByteArray(robotsContent), "text/plain",
							user_agent);
				}
			}
			return rules.isAllowed(page_url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static String getContents(String page_url,String user_agent) {
		try {
			URL oracle = new URL(page_url);
			BufferedReader in = new BufferedReader(new InputStreamReader(
					oracle.openStream()));
			String content = new String();
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				content += inputLine + "\n";
			}
			in.close();
			//System.out.println(content);
			return content;
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			return null;
		}
		return null;
	}
	
	public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return b.toByteArray();
    }

	private boolean extractDocumentInfo(ArrayList<String> topicwords, String url,
			HashMap<String, Integer> processedUrls, Frontier f, 
			HashMap<Integer, Boolean> defaultPorts, 
			HashMap<String, Boolean> discardHosts,
			HashMap<String, Boolean> spiderTrapDomains) throws IOException,JSONException {
		
		JSONObject obj = fetchInfoAsJson(url,spiderTrapDomains);
		if(obj==null) return false;
		ArrayList<String> outlinks = addOutlinksToFrontier
				(topicwords,url,obj.getJSONArray("outlinks"),f,processedUrls,defaultPorts,discardHosts,spiderTrapDomains);
		if(!sendToIndex(obj)) {
			System.out.println("Returned false "+url);
			return false;
		}
		writeToLinkGraph(url,outlinks);
		writeToAP89(obj.getString("id"),obj.getString("title"),obj.getString("text"));
		return true;
	}

	private boolean sendToIndex(JSONObject obj) throws JSONException, IOException {
		int docsize = docs.size();
		docs.put(obj.getString("id"), docsize);
		String url = "http://localhost:9200/index/document/" + docsize;
		HttpURLConnection urlc=null;
		try {
			urlc = (HttpURLConnection) new URL(url).openConnection();
			//System.out.println(new String(serialize(obj.get("rawHtml")),"US-ASCII"));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		urlc.setAllowUserInteraction( false );
		urlc.setDoInput( true );
		urlc.setDoOutput( true );
		urlc.setUseCaches( true );
		try{
			urlc.setRequestMethod("POST");
		
		urlc.connect();
		
		OutputStream output = urlc.getOutputStream();
		output.write(obj.toString().getBytes());
		output.flush();
		output.close();
		}
		catch(Exception e){
			System.out.println("\n"+e.toString());
		}
		if(!(urlc.getResponseCode() <=299 && urlc.getResponseCode() >=200))
			{
				PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter("responsecodeerror",true)));
				pw.println(obj.get("id")+" "+urlc.getResponseCode()+" "+urlc.getResponseMessage());
				pw.close();
			}
		return urlc.getResponseCode() <=299 && urlc.getResponseCode() >=200;
	}

	private int updateInlink(String url,String inlinkurl) throws MalformedURLException, JSONException {
		String link = "http://localhost:9200/index/document/" + docs.get(inlinkurl)+"/_update";
		HttpURLConnection urlc=null;
		try {
			urlc = (HttpURLConnection) new URL(link).openConnection();
			//System.out.println(obj.get("rawHtml"));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		urlc.setAllowUserInteraction( false );
		urlc.setDoInput( true );
		urlc.setDoOutput( true );
		urlc.setUseCaches( true );
		try{
			urlc.setRequestMethod("POST");
		
		urlc.connect();
		
		OutputStream output = urlc.getOutputStream();
		output.write(("{ \"script\" : \"ctx._source.inlinks += url\","
						+ "  \"params\" : {"
							+ " \"url\" : \""+url+"\"}}").getBytes());
		output.flush();
		output.close();
		return urlc.getResponseCode();
		}
		catch(Exception e){
			System.out.println("\n"+e.toString());
		}
		return 0;
		
	}
	
	private byte[] serializetoarray(Object  o)
	{
		byte[] yourBytes =null;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
		  out = new ObjectOutputStream(bos);   
		  out.writeObject(o);
		  yourBytes = bos.toByteArray();
		} 
		catch(Exception e) {
			System.out.println("xception "+e.toString());
		}
		return yourBytes;
	}
	
	private ArrayList<String> addOutlinksToFrontier(ArrayList<String> topicwords, String origurl,JSONArray jsonArray, Frontier f,
			HashMap<String, Integer> processedUrls, HashMap<Integer, Boolean> defaultPorts, 
			HashMap<String, Boolean> discardHosts,HashMap<String, Boolean> spiderTrapDomains) throws JSONException, IOException {
		Set<String> outlinks = new HashSet<String>();
		for(int i=0;i<jsonArray.length();i++)
		{
			String url = jsonArray.getString(i);
			url = canonicalizeURL(url,defaultPorts,discardHosts);
			if(url==null) continue;
			if(url.equals(origurl)) continue;
			if(url.contains("wikipedia.org") && !url.contains("en.wikipedia.org")) continue;
			outlinks.add(url);
		}
		
		removeRepeatingPaths(f,origurl,outlinks,spiderTrapDomains);
		
		Iterator<String> outset = outlinks.iterator();
		ArrayList<String> arr = new ArrayList<String>();
		while(outset.hasNext()) {
			String url = (String) outset.next();
			if(processedUrls.containsKey(url)) 
				{
				processedUrls.put(url, processedUrls.get(url)+1);
				updateInlink(origurl,url);
				}
			else 
			{
				if(f.containsUrl(url))
					f.incrementInlink(url, 1);
				else {
					arr.add(url);
					f.push(topicwords,url, 1);
				}
			}
		}
		
		ArrayList<String> res = (new ArrayList<String>());
		res.addAll(arr);
		return res;
	}

	private void removeRepeatingPaths(Frontier f, String url,Set<String> outlinks,HashMap<String, Boolean> spiderTrapDomains) throws IOException
	{
		Set<String> temp = new HashSet<String>(outlinks);
		String domain = extractQueryURL(url);
		//if(url.contains("www.gpsvisualizer.com/map_input?")) 
			//System.out.println("*************** "+domain);
		//HashMap<String,ArrayList<String>> commondomains = new HashMap<String,ArrayList<String>>();
		Iterator keys = outlinks.iterator();
		ArrayList<String> a  = new ArrayList<String>();
		while(keys.hasNext()){
			String key = (String) keys.next();
			URL u = new URL(key);
			if(key.contains(domain)) a.add(key);
		}
		
		//if(url.contains("www.gpsvisualizer.com/map_input?")) 
			//System.out.println("*************** "+a.size());
		
		
		if(a.size()>=2) {
			System.out.println("Added to trap "+domain+" "+a.size());
			spiderTrapDomains.put(domain,true);
			writeResDomain(domain);
		}
		
		Iterator domains = temp.iterator();
		while(domains.hasNext()){
			String dom = (String) domains.next();
			if(a.contains(dom)) 
					outlinks.remove(dom);
		}
		
		if(a.size()>=2) f.removeFromFrontier(domain);
		
		//if(a.size()>0) outlinks.add(a.get(0));
	}

	private void writeResDomain(String url) throws IOException {
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("RestrictedDomains.txt", true)));
		out.println(url);
		out.close();
	}
	
	private void writeToAP89(String id, String title, String text) throws IOException {
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("AP89.txt", true)));
		out.println("<DOC>");
		out.println("<DOCNO> "+id+" </DOCNO>");
		out.println("<HEAD> "+title+" </HEAD>");
		out.println("<TEXT> "+text+" </TEXT>");
		out.println("</DOC>");
		out.close();
	}

	private JSONObject fetchInfoAsJson(String url, HashMap<String, Boolean> spiderTrapDomains) throws JSONException, IOException{
		JSONObject obj = new JSONObject();
		JSONArray jarr = new JSONArray();
		Document document = null;
		try{
		document = Jsoup.connect(url).get();
		}
		catch(IOException e){
			System.out.println("Failed to connect to url for Jsoup "+url+" "+e.toString());
			spiderTrapDomains.put(extractQueryURL(url), true);
			writeResDomain(extractQueryURL(url));
		}
		
		obj.put("id",url);
		if(document==null || (document.head()==null && document.body()==null)) return null;
		obj.put("header",document.head().html().getBytes("UTF-8"));
		
		obj.put("title",document.title());
		obj.put("rawHtml",document.html().getBytes("UTF-8"));
		
		Elements links = document.select("a[href]");
        for (Element link : links)
        	jarr.put(link.attr("abs:href"));//.put(obj.put("link",link.attr("abs:href")));
        //obj.put("links",jarr);
        obj.put("outlinks",jarr);
        document = new Cleaner(Whitelist.simpleText()).clean(document);
	    
	    // Adjust escape mode
	    document.outputSettings().escapeMode(EscapeMode.xhtml);

	    // Get back the string of the body.
	    obj.put("text",document.body().text());//.html().replaceAll("\\\\n", "\n"));
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
			if((System.currentTimeMillis() - domainTimes.get(key)) > 5000)
			{
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
	(ArrayList<String> topicwords, Frontier f, HashMap<Integer, Boolean> defaultPorts, HashMap<String, Boolean> discardHosts) throws IOException {
		// Reading file for seed URLS
		BufferedReader br = new BufferedReader(new FileReader("seedurls"));
		String url="";
		while((url=br.readLine())!=null)
		{
			String curl = canonicalizeURL(url,defaultPorts,discardHosts);
			if(curl!=null) f.push(topicwords,curl, Integer.MAX_VALUE);
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
