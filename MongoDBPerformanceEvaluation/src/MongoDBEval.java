import com.mongodb.MongoClient;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBCollection;
import com.mongodb.DB;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


public class MongoDBEval 
{
	//Attributes for Config.xml file
	// TODO Auto-generated method stub
	private static String serverName;		/*Contains the Server Name*/
	private static int serverPort;			/*Contains the PORT Number for the Server*/
	private static String serverHostAddress;/*Contains the IP Address*/

	//Variables for fetching data from XML file
	private static Element element;
	private static Node nNode;

	@SuppressWarnings("deprecation")
	public static void main(String[] args)
	{
		try
		{
			//Change the path here for config file
			//Load the configuration file CONFIG.XML has information about all the servers
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

			//Repeat for all the servers in the config.xml file (In our case 16 Servers)
			//Setup MongoClient for every Peer in our system
			MongoClient mongoClient[] =  new MongoClient[nodeList.getLength()];

			for (int i = 1; i <= nodeList.getLength(); i++) 
			{
				nNode = nodeList.item(i-1);

				if (nNode.getNodeType() == Node.ELEMENT_NODE) 
				{
					element = (Element) nNode;

					//get all the attributes for each of the servers
					serverName = element.getElementsByTagName("ServerName").item(0).getTextContent();  
					serverPort = Integer.parseInt(element.getElementsByTagName("ServerPort").item(0).getTextContent());
					serverHostAddress = element.getElementsByTagName("ServerIP").item(0).getTextContent();

					mongoClient[i-1] =  new MongoClient(serverHostAddress, 27017);
				}
			}

			//Create Database DB, for every peer
			DB dtBase[] = new DB[nodeList.getLength()];

			//Create a Collection for every peer
			DBCollection collectionTable[] =new DBCollection[nodeList.getLength()];

			for(int i=1;i<=nodeList.getLength();i++)
			{
				dtBase[i-1] = mongoClient[i-1].getDB("EvalMongoDB");
				collectionTable[i-1] = dtBase[i-1].getCollection("MongoDBCollection");
			}

			System.out.println("Performing PUT Operation");
			String key, value;
			int generatedHashCode;
			int random = new Random().nextInt(16)+1;
			long startTime, endTime;
			startTime = System.currentTimeMillis();
			for(int i=100000 ; i<2000000 ; i++)
			{
				key = padKey(String.valueOf(random*i));
				value = padKey(String.valueOf("RandomValue"+i));
				
				generatedHashCode=hashFunction(i);
				
				//creating a Document, for Put operation, containing Key, value pair
				BasicDBObject documentKeyValue = new BasicDBObject();
				documentKeyValue.put(key, value);
				
				//add the document to the collection created earlier
				collectionTable[generatedHashCode].insert(documentKeyValue);
			}
			endTime = System.currentTimeMillis();
			System.out.println("Time taken for PUT operation is: "+(endTime-startTime)/1000+" seconds");
			
			
			/*GET OPERATION*/
			System.out.println("Performing GET Operation");
			startTime = System.currentTimeMillis();
			for(int i=100000 ; i<2000000 ; i++)
			{
				key = padKey(String.valueOf(random*i));
				value = padKey(String.valueOf("RandomValue"+i));
				
				generatedHashCode=hashFunction(i);
				
				//creating a Document, for Put operation, containing Key, value pair
				BasicDBObject documentKeyValue = new BasicDBObject();
				documentKeyValue.put(key, value);
							
				//read the value at that key, using cursor
				DBCursor readCursor = collectionTable[generatedHashCode].find(documentKeyValue);
				readCursor.next();
			}
			endTime = System.currentTimeMillis();
			System.out.println("Time taken for GET operation is: "+(endTime-startTime)/1000+" seconds");
			
			/*DELETE OPERATION*/
			System.out.println("Performing DELETE Operation");
			startTime = System.currentTimeMillis();
			for(int i=100000 ; i<2000000 ; i++)
			{
				key = padKey(String.valueOf(random*i));
				value = padKey(String.valueOf("RandomValue"+i));
				
				generatedHashCode=hashFunction(i);
				
				//creating a Document, for Put operation, containing Key, value pair
				BasicDBObject documentKeyValue = new BasicDBObject();
				documentKeyValue.put(key, value);
							
				//delete the Key
				collectionTable[generatedHashCode].findAndRemove(documentKeyValue);
			}
			endTime = System.currentTimeMillis();
			System.out.println("Time taken for DELETE operation is: "+(endTime-startTime)/1000+" seconds");
			
		}
		catch(IOException | ParserConfigurationException | SAXException e)
		{
			System.out.println("Error in Code while setting up !!!");
		}
	}

	//Calculate HashValue
	public static int hashFunction(int i)
	{
		int hashCode;
		
		hashCode = i%16;
		return hashCode;
	}
	
	//Pad Key, Value pairs, 10 bytes: key, 90 bytes: value
	public static String padKey(String key)
	{
		for(int i=key.length();i<10;i++)
		{
			key+="*";
		}
		return key;
	}

	public static String padValue(String value)
	{
		for(int i=value.length();i<90;i++)
		{
			value+="*";
		}
		return value;
	}
}