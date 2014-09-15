package com.jiaxing.parser;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;




public class Parser {
	public static List<String> extractLink(String jsonStr){
		List<String> resultList = new LinkedList<String>();
		ObjectMapper mapper = new ObjectMapper();
		try {
			JsonNode node = mapper.readValue(jsonStr, JsonNode.class);
			List<JsonNode> linksList = node.findValues("links");
			for(JsonNode links : linksList){
				for(JsonNode link : links){
					if(link.has("type") && link.findValue("type").getTextValue().equals("a")){
						if(link.has("href")){
							resultList.add(link.findValue("href").getTextValue());
						}
					}
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
		}catch(Exception e){
			e.printStackTrace();
			System.out.println(jsonStr);
			System.exit(1);
		}
		return resultList;
	}
	
	public static void main(String[] args){
		Parser parser = new Parser();
		System.out.println(parser.extractLink("test.txt"));
	}
}
