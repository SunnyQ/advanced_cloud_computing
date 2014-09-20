package com.jiaxing.utils;

import java.net.URLDecoder;
import java.net.URLEncoder;

public class Utils {
	public static String encodeUrl(String inputUrl){
		return URLEncoder.encode(inputUrl);
	}
	
	public static String decodeUrl(String inputUrl){
		return URLDecoder.decode(inputUrl);
	}
}
