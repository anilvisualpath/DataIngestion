import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
//import org.apache.commons.io.FileUtils;

public class StockDataIngestion {

   public static void main(String[] args) {
	try {
			// Read the properties from Properties file
			Properties CProp = new Properties();
			//save properties to project root folder
			//CProp.store(new FileOutputStream("config.properties"), null);
			CProp.load(new FileInputStream("/home/hduser/stockproperties"));
			String FilesPath = "";
			String OutPutFile = "";
			String StockSymbolsFile = "";
			String SpecialTagsFile = "";
			String SpecialTags = "";
			String StartYear = "";
			String EndYear = "";
			String StartAlphabet = "";
			String EndAlphabet = "";
			System.out.println("BEFORE FOR");
			for(String key : CProp.stringPropertyNames()) 
			{
				String strValue = CProp.getProperty(key);
				System.out.println(key + " => " + strValue);
		  
			      switch(key) {
					case "files_path":
						FilesPath = strValue;
						System.out.println("files_path = " + FilesPath);
						break;
				case "Output_file":
						OutPutFile = strValue;
						System.out.println("OutPutFile = " + OutPutFile);
						break;
				case "Stock_symbols_file":
						StockSymbolsFile = strValue;
					System.out.println("StockSymbolsFile = " + StockSymbolsFile);
						break;
				case "Special_tags_file":
						SpecialTagsFile = strValue;
					System.out.println("SpecialTagsFile = " + SpecialTagsFile);
						break;
				case "Special_tags":
						SpecialTags = strValue;
						System.out.println("SpecialTags = " + SpecialTags);
						break;

				case "Start_year":
						StartYear = strValue;
						System.out.println("StartYear = " + StartYear);
						break;
				case "End_year":
						EndYear = strValue;
						System.out.println("EndYear = " + EndYear);
						break;
				case "Symbol_start_alphabet":
						StartAlphabet = strValue;
						System.out.println("StartAlphabet = " + StartAlphabet);
						break;
				case "Symbol_end_alphabet":
						EndAlphabet = strValue;
						System.out.println("EndAlphabet = " + EndAlphabet);
						break;
				}
		  
			}
			OutPutFile = FilesPath + "/" + OutPutFile;
			System.out.println("Downloading " + OutPutFile + " document...");

			//Build the URL using Stock symbols and Special tags
			String BaseURL = "http://finance.yahoo.com/d/quotes.csv?";
			String SpecialTagsURL = "";
			SpecialTagsURL = "&f=" + SpecialTags;
			FileFromUrl(FilesPath + "/" + StockSymbolsFile, FilesPath + "/" + SpecialTagsFile, BaseURL, SpecialTagsURL, OutPutFile);
			
			
			System.out.println("Downloaded " + OutPutFile + " document");
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

// Using Java IO
public static void FileFromUrl(String StockSymbolsFile, String SpecialTagsFile, String BaseURL, String SpecialTagsURL, String OutPutFile) throws IOException
{
	String StockURL = "";
	BufferedReader URLIn = null;
	BufferedWriter URLOut = null;
	try {
		  FileInputStream fstream = new FileInputStream(StockSymbolsFile);
		  // Get the object of DataInputStream
		  DataInputStream in = new DataInputStream(fstream);
		  BufferedReader br = new BufferedReader(new InputStreamReader(in));
		  String strLine;
		  //Read File Line By Line
		  String StockSysmbolsURL = "";
		  		  
		  int count = 0;			
		  	
			URLOut = new BufferedWriter(new FileWriter(OutPutFile));
		  
		  while ((strLine = br.readLine()) != null)   {

		      String[] delims = strLine.split(",");
		      String first = delims[0];
		      System.out.println("First word: "+first);
		      StockSysmbolsURL = StockSysmbolsURL + first;  
			  		       
		      //System.out.println("StockSysmbolsURL= " +StockSysmbolsURL);
		      StockURL = BaseURL + StockSysmbolsURL + SpecialTagsURL;
		      //System.out.println("StockURL= " +StockURL);		      
		  	  URL url;
		  		if(count >1)	
		  		{	
		  		url = new URL(StockURL);
		  		URLIn = new BufferedReader(new InputStreamReader(url.openStream()));
		  			
		  			char[] cbuf=new char[300];
		  			while ((URLIn.read(cbuf)) != -1) {
		  			URLOut.write(cbuf);
		  			}
		  		}
		  	  StockSysmbolsURL = "";
		      if(count >=220) break;
		      count++;
		  }
			
		    
		} //try ends
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			if(URLIn != null) URLIn.close();
			if(URLOut != null) URLOut.close();	
		}

  } //BuildURL() ends 
 }


