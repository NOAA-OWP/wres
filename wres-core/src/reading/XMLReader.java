/**
 * 
 */
package reading;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamConstants;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * @author Tubbs
 *
 */
public class XMLReader {

	/**
	 * 
	 */
	public XMLReader(String filename) {
		this.filename = filename;		
	}
	
	protected String get_filename()
	{
		return filename;
	}
	
	public void parse()
	{
		try
		{
			XMLStreamReader reader = create_reader();
			
			while (reader.hasNext())
			{
				parse_element(reader);
				reader.next();
			}
			
			reader.close();
			
		}
		catch (XMLStreamException | FileNotFoundException error)
		{
			error.printStackTrace();
		}
	}
	
	protected String tag_value(XMLStreamReader reader) throws XMLStreamException
	{
		String value = null;
		
		if (reader.isStartElement() && (reader.next() == XMLStreamConstants.CHARACTERS))
		{
			//int begin_index = reader.getTextStart();
			//int end_index = reader.getTextLength();
			value = reader.getText().trim();
		}
		
		return value;
	}
	
	protected XMLStreamReader create_reader() throws FileNotFoundException, XMLStreamException
	{
		XMLInputFactory factory = XMLInputFactory.newFactory();
		return factory.createXMLStreamReader(new FileReader(get_filename()));
	}
	
	protected void parse_element(XMLStreamReader reader)
	{
		switch (reader.getEventType())
		{
		case XMLStreamConstants.START_DOCUMENT:
			System.out.println("Start of the document");
			break;
		case XMLStreamConstants.START_ELEMENT:
			System.out.println("Start element = '" + reader.getLocalName() + "'");
			break;
		case XMLStreamConstants.CHARACTERS:
			int begin_index = reader.getTextStart();
			int end_index = reader.getTextLength();
			String value = new String(reader.getTextCharacters(), begin_index, end_index).trim();
			
			if (!value.equalsIgnoreCase(""))
			{
				System.out.println("Value = '" + value + "'");
			}
			
			break;
		case XMLStreamConstants.END_ELEMENT:
			System.out.println("End element = '" + reader.getLocalName() + "'");
			break;
		case XMLStreamConstants.COMMENT:
			if (reader.hasText())
			{
				System.out.println(reader.getText());
			}
			break;
		}
	}

	private String filename = "";
}
