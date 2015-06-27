package com.ir.webcrawler; 
import java.io.*;
import java.net.*;
import java.util.ArrayList;
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

public class CrawlerModif {
	private HashMap<String,Integer> docs ;
	Map<String, BaseRobotRules> robotsTxtRules = null;

	public static void main(String args[]) throws Exception
	{
		PrintWriter out = new PrintWriter("LinkGraph.txt");
		PrintWriter pw = new PrintWriter("AP89.txt");
		out.close();
		pw.close();

		HashMap<Integer,Boolean> defaultPorts;
		HashMap<String,Boolean> discardHosts;
		HashMap<String, Boolean> spiderTrapDomains;

		CrawlerModif cw = new CrawlerModif();

		cw.robotsTxtRules = new HashMap<String, BaseRobotRules>();
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
		discardHosts.put("www.wikimediafoundation.org",true);
		discardHosts.put("tools.wmflabs.org", true);

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
		HashMap<String,ArrayList<String>> inlinks = new HashMap<String,ArrayList<String>>();
		
		while(f.size() > 0 && docs.size()<20000){
			System.out.println("Docs processed = "+docs.size()+" Frontier size = "+f.size());
			manageURLS(topicwords,f,domainTimes,processedUrls,inlinks,defaultPorts,discardHosts,spiderTrapDomains);
		}
		
		writeInlinksToFile(inlinks);
	}

	private void writeInlinksToFile(HashMap<String, ArrayList<String>> inlinks) throws FileNotFoundException {
		Iterator<String> keys = inlinks.keySet().iterator();
		PrintWriter pw = new PrintWriter("Inlinks");
		while(keys.hasNext()){
			String key = (String) keys.next();
			pw.print(key+"\t");
			ArrayList<String> a = inlinks.get(key);
			for(int i=0;i<a.size();i++)
				pw.print(a.get(i)+"\t");
			pw.println();
		}
		pw.close();
	}

	private void manageURLS(ArrayList<String> topicwords,Frontier f, HashMap<String, Long> domainTimes, HashMap<String, Integer> processedUrls, 
			HashMap<String, ArrayList<String>> inlinks, HashMap<Integer, Boolean> defaultPorts, HashMap<String, Boolean> discardHosts, 
			HashMap<String, Boolean> spiderTrapDomains) throws InterruptedException, IOException {
		Tuple t = f.pop(topicwords);
		ArrayList<Tuple> temp = new ArrayList<Tuple>();

		while((spiderTrapDomains.containsKey(extractQueryURL(t.getUrl())) 
				|| !isValidDomain(domainTimes,new Url(t.getUrl()).getHost())) && f.size() > 0)
		{
			if(t.getInlink() == Integer.MAX_VALUE) 
			{
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
			if(processUrl(domainTimes,topicwords,t,processedUrls,f,defaultPorts,discardHosts,inlinks,spiderTrapDomains)){
				if(t.getInlink() >= Integer.MAX_VALUE)
					processedUrls.put(t.getUrl(),0);
				else processedUrls.put(t.getUrl(),t.getInlink());
			}
			domainTimes.put(new Url(t.getUrl()).getHost(), System.currentTimeMillis());
		}
		else {
			//System.out.println("SLEEPING!"+" "+t.getUrl()+" "+t.getInlink()+" "+f.size());
			Thread.sleep(1000l);
			temp.add(new Tuple(t.getUrl(), t.getInlink(),t.getPriority()));
			f.add(temp);
		}
		removeAllowableDomains(domainTimes);
	}

	private boolean processUrl(HashMap<String,Long> domainTimes,ArrayList<String> topicwords, Tuple t, HashMap<String, Integer> processedUrls, Frontier f, 
			HashMap<Integer, Boolean> defaultPorts, HashMap<String, Boolean> discardHosts
			,HashMap<String, ArrayList<String>> inlinks, HashMap<String, Boolean> spiderTrapDomains) throws InterruptedException, IOException {

		String user_agent = "whatever";
		String url = t.getUrl();
		
		if(canBeRobotParsed(url,domainTimes))
		{
			System.out.println("Processing "+url+" "+t.getInlink());
			try{
				return extractDocumentInfo(user_agent,inlinks,domainTimes,topicwords,url,processedUrls,f,defaultPorts,discardHosts,spiderTrapDomains);
			}
			catch(IOException e) {
				System.out.println("url could not be parsed "+url+" "+e.toString());
				return false;
			}
			catch(JSONException e) {
				System.out.println("JSON could not be parsed "+e.toString());
				return false;
			}
		}
		return false;
	}

	private boolean canBeRobotParsed(String url,HashMap<String, Long> domainTimes) 
	{
		String user_agent ="whatever";
		try {
			return (isCrawlable(url,user_agent,domainTimes));
		} catch (InterruptedException e) {
			return false;
		}
	}

	private String extractQueryURL(String url) throws IOException{
		URL u = new URL(url);
		String domain = u.getHost()+(u.getPort()==-1? "":(":"+u.getPort()))+(u.getPath().equals("")?"/":u.getPath())+"?";
		return domain;
	}

	public boolean isCrawlable(String page_url, String user_agent, HashMap<String, Long> domainTimes) throws InterruptedException
	{
		//System.out.println("Start Crawling");
		try {
			URL urlObj = new URL(page_url);
			String hostId = urlObj.getProtocol() + "://" + urlObj.getHost()
					+ (urlObj.getPort() > -1 ? ":" + urlObj.getPort() : "");
			//System.out.println(hostId);
			BaseRobotRules rules = robotsTxtRules.get(hostId);
			if (rules == null) {
				System.out.println("Host not present "+urlObj.getHost());

				String robotsContent = getContents(hostId + "/robots.txt",user_agent);
				domainTimes.put(urlObj.getHost(),System.currentTimeMillis());
				if (robotsContent == null) {
					rules = new SimpleRobotRules(RobotRulesMode.ALLOW_ALL);
					robotsTxtRules.put(hostId, rules);
				} else {
					SimpleRobotRulesParser robotParser = new SimpleRobotRulesParser();
					rules = robotParser.parseContent(hostId,
							IOUtils.toByteArray(robotsContent), "text/plain",
							user_agent);
					robotsTxtRules.put(hostId, rules);
				}
			}
			//else System.out.println("Host present "+urlObj.getHost());
			return rules.isAllowed(page_url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static String getContents(String page_url,String user_agent) throws IOException {
		HttpURLConnection urlc = null;
		if(page_url=="") return null;
		try {
			urlc = (java.net.HttpURLConnection) new URL(page_url).openConnection();
			//System.out.println(new String(serialize(obj.get("rawHtml")),"US-ASCII"));
		} catch (Exception e1) {
			return null;
		}
		urlc.setAllowUserInteraction( false );
		urlc.setDoInput( true );
		urlc.setDoOutput( false );
		urlc.setUseCaches( true );
		urlc.setReadTimeout(5);
		byte b[]=null;
		try{
			urlc.setRequestMethod("GET");
			urlc.connect();
			b = new byte[10000];
			urlc.getInputStream().read(b);
		}
		catch(Exception e){
			return null;
		}
		//System.out.println(new String(b,"US-ASCII"));
		return new String(b,"US-ASCII");
		/*String content = new String();
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				content += inputLine + "\n";
			}
			in.close();
			//System.out.println(content);
			return content;		*/
		//return null
	}

	public static byte[] serialize(Object obj) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream o = new ObjectOutputStream(b);
		o.writeObject(obj);
		return b.toByteArray();
	}

	private boolean extractDocumentInfo(String user_agent,HashMap<String, ArrayList<String>> inlinks, HashMap<String,Long> domainTimes,
			ArrayList<String> topicwords, String url,
			HashMap<String, Integer> processedUrls, Frontier f, 
			HashMap<Integer, Boolean> defaultPorts, 
			HashMap<String, Boolean> discardHosts,
			HashMap<String, Boolean> spiderTrapDomains) throws IOException,JSONException {

		JSONObject obj = fetchInfoAsJson(url,spiderTrapDomains);
		if(obj==null) return false;
		if(inlinks.containsKey(url)) obj = addInlinksToJson(obj,inlinks.get(url));
		if(inlinks.containsKey(url)) inlinks.remove(url);
		ArrayList<String> outlinks = addOutlinksToFrontier
				(user_agent,domainTimes,topicwords,url,obj.getJSONArray("out_links"),f,processedUrls
						,defaultPorts,discardHosts,spiderTrapDomains,inlinks);
		if(!sendToIndex(obj)) {
			System.out.println("Returned false "+url);
			return false;
		}
		if(outlinks!=null) writeToLinkGraph(url,outlinks);
		writeToAP89(obj.getString("docno"),obj.getString("title"),obj.getString("text"));
		return true;
	}

	private JSONObject addInlinksToJson(JSONObject obj, ArrayList<String> links) throws JSONException {
		JSONArray arr = new JSONArray();
		for(int i=0;i<links.size();i++)
			arr.put(links.get(i));
		try {
			obj.put("in_links", arr);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Added inlinks for " + obj.getString("docno"));
		return obj;
	}

	private boolean sendToIndex(JSONObject obj) throws JSONException, IOException {
		int docsize = docs.size();
		docs.put(obj.getString("docno"), docsize);
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
			pw.println(obj.get("docno")+" "+urlc.getResponseCode()+" "+urlc.getResponseMessage());
			pw.close();
		}
		return urlc.getResponseCode() <=299 && urlc.getResponseCode() >=200;
	}

	private void updateInlink(HashMap<String, ArrayList<String>> inlinks, String url,String inlinkurl) throws MalformedURLException, JSONException {
		ArrayList<String> a = inlinks.containsKey(inlinkurl)  ? inlinks.get(inlinkurl) : new ArrayList<String>();
		a.add(url);
		inlinks.put(inlinkurl, a);
		/*String link = "http://localhost:9200/index/document/" + docs.get(inlinkurl)+"/_update";
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
*/
	}

	private ArrayList<String> addOutlinksToFrontier(String user_agent,HashMap<String,Long> domainTimes,ArrayList<String> topicwords, String origurl,JSONArray jsonArray, Frontier f,
			HashMap<String, Integer> processedUrls, HashMap<Integer, Boolean> defaultPorts, 
			HashMap<String, Boolean> discardHosts,HashMap<String, Boolean> spiderTrapDomains, 
			HashMap<String, ArrayList<String>> inlinks) throws JSONException, IOException{
		Set<String> outlinks = new HashSet<String>();
		long time = System.currentTimeMillis();
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
				updateInlink(inlinks,origurl,url);
			}
			else 
			{
				if(f.containsUrl(url))
						f.incrementInlink(url, 1);
				else {
					arr.add(url);
					f.push(topicwords,url, 1);
				}
				ArrayList<String> a = inlinks.containsKey(url) ? inlinks.get(url) : new ArrayList<String>();
				a.add(origurl);
				inlinks.put(url, a);
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
		boolean found = false;
		ArrayList<String> a  = new ArrayList<String>();
		while(keys.hasNext()){
			String key = (String) keys.next();
			if(key.contains(domain)) {
				System.out.println("Added to trap "+domain+" "+a.size());
				spiderTrapDomains.put(domain,true);
				writeResDomain(domain);
				found = true;
				break;
			}
		}

		Iterator domains = temp.iterator();
		while(domains.hasNext()){
			String dom = (String) domains.next();
			if(a.contains(dom)) 
				outlinks.remove(dom);
		}

		if(found) f.removeFromFrontier(domain);
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

		obj.put("docno",url);
		if(document==null || (document.head()==null && document.body()==null)) return null;
		obj.put("HTTPheader",document.head().html());

		obj.put("title",document.title());
		obj.put("html_Source",document.html());

		Elements links = document.select("a[href]");
		for (Element link : links)
			jarr.put(link.attr("abs:href"));//.put(obj.put("link",link.attr("abs:href")));
		//obj.put("links",jarr);
		obj.put("out_links",jarr);
		document = new Cleaner(Whitelist.simpleText()).clean(document);

		// Adjust escape mode
		document.outputSettings().escapeMode(EscapeMode.xhtml);

		// Get back the string of the body.
		obj.put("text",document.body().text());//.html().replaceAll("\\\\n", "\n"));
		obj.put("author", "p");
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

		if(host.startsWith("www."))
			host = host.substring(4);
		
		if(query !="") 
			query = "?"+query;

		if(!defaultPorts.containsKey(port) && port!=-1)
			portstr = ":"+port;

		return protocol.toLowerCase()+"://"+host.toLowerCase()+portstr+filename;
	}
}
