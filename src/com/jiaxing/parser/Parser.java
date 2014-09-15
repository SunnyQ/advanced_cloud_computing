package com.jiaxing.parser;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;




public class Parser {
	public static List<String> extractLink(String jsonStr){
		List<String> resultList = new LinkedList<String>();
		ObjectMapper mapper = new ObjectMapper();
		try {
			Map<String, Object> rawData = mapper.readValue(new File(jsonStr), Map.class);
			LinkedHashMap<String, Object> temp = ((LinkedHashMap<String, Object>)rawData.get("content"));
			List<Map> links = (List<Map>)temp.get("links");
			for(Map<String, String> map : links){
				if(map.containsKey("type") && map.get("type").equals("a") && map.containsKey("href")){
					resultList.add(map.get("href"));
				}
			}
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return resultList;
	}
	
	public static void main(String[] args){
		Parser parser = new Parser();
		System.out.println(parser.extractLink("test.txt"));
	}
}
