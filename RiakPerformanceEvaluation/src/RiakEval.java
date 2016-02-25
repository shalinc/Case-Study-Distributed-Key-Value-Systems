import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.DeleteValue;

import java.io.File;
import java.net.UnknownHostException;
import java.util.Random;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class RiakEval
{
	//Attributes for Config.xml file
	// TODO Auto-generated method stub
	private static String serverName;		/*Contains the Server Name*/
	private static int serverPort;			/*Contains the PORT Number for the Server*/
	private static String serverHostAddress;/*Contains the IP Address*/

	//Variables for fetching data from XML file
	private static Element element;
	private static Node nNode;


	public static void main(String[] args) 
	{
		try 
		{
			String serverArgs = args[0];
			long startTime, endTime;
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

					if(serverName.equalsIgnoreCase(serverArgs))
					{
						//Create a Bucket or NameSpace, for Riak
						Namespace riakBucket = new Namespace("RiakTable");

						//Setting up the Riak Cluster
						RiakCluster cluster = setUpRiakCluster(serverHostAddress);

						//setting up Riak Client, to join cluster created
						RiakClient client = new RiakClient(cluster);

						System.out.println("Performing 100K PUT requests");

						//createing the Object Location and Riak Object, and StoreValue object
						Location riakObjectLocation = null;
						RiakObject riakObject = new RiakObject().setContentType("text/plain");
						StoreValue storeKeyValue=null;

						String key, value;
						int random = new Random().nextInt(16)+1;

						startTime = System.currentTimeMillis();
						for(int j = 100000; i<200000; i++)
						{
							//generate Key value pairs
							key = padKey(String.valueOf(j*random));
							value = padValue(String.valueOf("RandomValue"+j));

							//set the Key, into the riakBucket
							riakObjectLocation = new Location(riakBucket, key);

							//set the value for the RiakObject, i.e. create an object having VALUE
							riakObject.setValue(BinaryValue.create(value));

							//Build the Query for Storing the Key/Value pair
							storeKeyValue = new StoreValue.Builder(riakObject).withLocation(riakObjectLocation).build();

							//Execute the Query
							client.execute(storeKeyValue);

						}
						endTime = System.currentTimeMillis();
						System.out.println("Time taken for PUT: " + (endTime-startTime)/1000 + " seconds");

						System.out.println("Performing 100K GET requests");

						startTime = System.currentTimeMillis();

						for(int j= 100000;i<200000;i++)
						{
							//generate Key value pairs to get
							key = padKey(String.valueOf(j*random));
							//value = padValue(String.valueOf("RandomValue"+j));

							//set the Key, into the riakBucket
							riakObjectLocation = new Location(riakBucket, key);

							//Fetching the Value for the Key
							FetchValue fetchKeyValue = new FetchValue.Builder(riakObjectLocation).build();

							//Execute the Riak Object Query for Fetch
							RiakObject fetchedObjectValue = client.execute(fetchKeyValue).getValue(RiakObject.class);

							//check if the value fetched is same as asked for
							assert(fetchedObjectValue.getValue().equals(riakObject.getValue()));
						}
						endTime = System.currentTimeMillis();

						System.out.println("Time taken for GET: " + (endTime-startTime)/1000 + " seconds");

						System.out.println("Performing 100K DEL requests");

						startTime = System.currentTimeMillis();

						for(int j = 100000;i<200000;i++)
						{
							//generate Key value pairs to get
							key = padKey(String.valueOf(j*random));
							//value = padValue(String.valueOf("RandomValue"+j));

							//set the Key, into the riakBucket
							riakObjectLocation = new Location(riakBucket, key);

							//Execute the Query for delete in RiakObject
							DeleteValue deleteValue = new DeleteValue.Builder(riakObjectLocation).build();
							client.execute(deleteValue);
						}
						endTime = System.currentTimeMillis();

						System.out.println("DEL operations in " + (endTime-startTime)/1000 + " seconds.");

						//ShutDown cluster object 
						cluster.shutdown();		
						break;
					}
				}	    
			}
		} 
		catch (Exception e) 
		{
			System.out.println("Error in Setting up Nodes for Cluster");
		}
	}

	//Creating a client Object to Interact with Riak 
	private static RiakCluster setUpRiakCluster(String IPAddress) throws UnknownHostException 
	{
		RiakNode nodeRiak = new RiakNode.Builder().withRemoteAddress(IPAddress).withRemotePort(5005).build();

		//Create a Cluster with Node created above, passing RiakNode object
		RiakCluster cluster = new RiakCluster.Builder(nodeRiak).build();

		//start the cluster for program execution
		cluster.start();

		return cluster;
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