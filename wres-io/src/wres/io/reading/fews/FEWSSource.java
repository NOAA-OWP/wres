package wres.io.reading.fews;

import wres.io.reading.BasicSource;
import wres.io.reading.XMLReader;

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
	public void saveForecast() {
		XMLReader source_reader = new PIXMLReader(this.getFilename());
		source_reader.parse();		
	}

	@Override
	public void saveObservation() {
		XMLReader source_reader = new PIXMLReader(this.getAbsoluteFilename(), false);
		source_reader.parse();		
	}

}
