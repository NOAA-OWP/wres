/**
 * 
 */
package reading.fews;

import reading.BasicSource;
import reading.XMLReader;

/**
 * @author Christopher Tubbs
 * Interprets a FEWS (PIXML) source into either forecast or observation data and stores them in the database
 */
public class FEWSSource extends BasicSource {

	/**
	 * Constructor
	 */
	public FEWSSource() {}
	
	/**
	 * Constructor that sets the filename 
	 * @param filename The name of the source file
	 */
	public FEWSSource(String filename)
	{
		this.set_filename(filename);
	}

	@Override
	public void save_forecast() {
		XMLReader source_reader = new PIXMLReader(this.get_filename());
		source_reader.parse();		
	}

	@Override
	public void save_observation() {
		XMLReader source_reader = new PIXMLReader(this.get_absolute_filename(), false);
		source_reader.parse();		
	}

}
