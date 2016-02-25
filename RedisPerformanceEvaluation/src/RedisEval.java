import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

public class RedisEval
{
	//Attributes for Config.xml file
	// TODO Auto-generated method stub
	private static String serverName;		/*Contains the Server Name*/
	private static int serverPort;			/*Contains the PORT Number for the Server*/
	private static String serverHostAddress;/*Contains the IP Address*/

	//Variables for fetching data from XML file
	private static Element element;
	private static Node nNode;

	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		//Load the ConfigFile Ubuntu
		try
		{
			File configFile = new File("src/config.xml");
			//File configFile = new File("C:\\Users\\USER\\Desktop\\PROG2_CHOPRA_SHALIN\\SimpleDistributedHashTable\\src\\config.xml");	

			//Parsing the DOM tree for XML file
			DocumentBuilderFactory docbldFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder;
			docBuilder = docbldFactory.newDocumentBuilder();
			Document doc = docBuilder.parse(configFile);

			//normalize the DOM tree
			doc.getDocumentElement().normalize();

			//get the list of servers and their information from XML <Servers> tag
			NodeList nodeList = doc.getElementsByTagName("Servers");

			//Create a set of JedisClusterNodes
			Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();

			//Repeat for all the servers in the config.xml file (In our case 16 Servers)
			//Jedis Cluster Creation
			for (int i = 0; i < nodeList.getLength(); i++) 
			{
				nNode = nodeList.item(i);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) 
				{
					element = (Element) nNode;

					//get all the attributes for each of the servers
					serverName = element.getElementsByTagName("ServerName").item(0).getTextContent();  
					serverPort = Integer.parseInt(element.getElementsByTagName("ServerPort").item(0).getTextContent());
					serverHostAddress = element.getElementsByTagName("ServerIP").item(0).getTextContent();

					//add the IP and Port to the Jedis Cluster
					jedisClusterNodes.add(new HostAndPort(serverHostAddress, serverPort));
				}	    
			}

			//Create a JedisCluster Object and add the clusterNodes formed earlier
			JedisCluster jedisClus = new JedisCluster(jedisClusterNodes);

			/*PERFORMING PUT*/
			System.out.println("NOW PERFORMING 100K PUT OPERATIONS");
			String key, value;
			long startTime, endTime;

			//Generation of Random, Key value pair
			int random = new Random().nextInt(16)+1;
			startTime = System.currentTimeMillis();

			for(int i=100000; i<200000;i++)
			{
				key = padKey(String.valueOf(i*random));
				value = padValue("RandomValue"+i);

				//set to put values in DHT
				jedisClus.set(key,value);
			}
			endTime = System.currentTimeMillis();
			System.out.println("PUT TIME TAKEN" + (endTime - startTime)/1000 + " seconds");

			/*PERFORMING GET*/
			System.out.println("NOW PERFORMING 100K GET OPERATIONS");
			startTime = System.currentTimeMillis();

			for(int i=100000; i<=200000;i++)
			{
				key = padKey(String.valueOf(i*random));
				//value = padValue("randomValue:"+i);

				//get to get values FROM DHT
				jedisClus.get(key);
			}
			endTime = System.currentTimeMillis();
			System.out.println("GET TIME TAKEN" + (endTime - startTime)/1000 + " seconds.");

			/*PERFORMING DELETE*/
			System.out.println("NOW PERFORMING 100K DEL OPERATIONS");

			startTime = System.currentTimeMillis();

			for(int i=100000;i<200000;i++)
			{
				key = padKey(String.valueOf(i*random));

				//delete values FROM DHT
				jedisClus.del(key);
			}
			endTime = System.currentTimeMillis();
			System.out.println("DEL TIME TAKEN" + (endTime - startTime)/1000 + " seconds.");

		}catch(IOException | ParserConfigurationException | SAXException e)
		{
			e.printStackTrace();
		}
	}

	/* The entire message i.e KEY + VALUE is of 100 bytes
	 * out of which KEY is of 10 Bytes, here we pad the remaining bytes of the key with "*" 
	 * while sending we send entire 100 bytes*/
	public static String padKey(String key)
	{
		for(int i=key.length();i<10;i++)
		{
			key+="*";
		}
		return key;
	}

	/* The entire message i.e KEY + VALUE is of 100 bytes
	 * out of which VALUE is of 90 Bytes, here we pad the remaining bytes of the value with "*" 
	 * while sending we send entire 100 bytes*/
	public static String padValue(String value)
	{
		for(int i=value.length();i<90;i++)
		{
			value+="*";
		}
		return value;
	}
}
