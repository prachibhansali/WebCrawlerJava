package com.ir.webcrawler;
import java.net.*;

public class Url {
	String url;
	String host;
	String protocol;
	String query;
	String path;
	
	public Url(String url) throws MalformedURLException {
		this.url = url;
		setHost();
		setProtocol();
	}
	
	public void setProtocol() throws MalformedURLException {
		protocol = new URL(url).getProtocol();
	}
	
	public String getProtocol()
	{
		return protocol;
	}
	
	public void setQuery() throws MalformedURLException {
		query = new URL(url).getQuery();
	}
	
	public String getQuery()
	{
		return query;
	}
	
	public String getUrl()
	{
		return url;
	}
	
	public void setUrl(String url)
	{
		this.url = url;
	}
	
	public String getHost()
	{
		return host;
	}
	
	public void setHost() throws MalformedURLException
	{
		URL u = new URL(url);
		this.host = u.getHost();
	}
}
