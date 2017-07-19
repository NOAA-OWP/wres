package wres.io.reading.fews;

import wres.io.reading.BasicSource;
import wres.io.reading.XMLReader;

import java.io.IOException;

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
		this.setFilename(filename);
	}

	@Override
	public void saveForecast() throws IOException {
		XMLReader sourceReader = new PIXMLReader(this.getFilename());
		sourceReader.parse();
	}

	@Override
	public void saveObservation() throws IOException {
		XMLReader sourceReader = new PIXMLReader(this.getAbsoluteFilename(), false);
		sourceReader.parse();
	}

}
