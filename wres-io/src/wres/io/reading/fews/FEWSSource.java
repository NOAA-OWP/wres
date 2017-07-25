package wres.io.reading.fews;

import wres.io.reading.BasicSource;
import wres.util.Internal;

import java.io.IOException;

/**
 * @author Christopher Tubbs
 * Interprets a FEWS (PIXML) source into either forecast or observation data and stores them in the database
 */
@Internal(exclusivePackage = "wres.io")
public class FEWSSource extends BasicSource {

	/**
	 * Constructor that sets the filename 
	 * @param filename The name of the source file
	 */
    @Internal(exclusivePackage = "wres.io")
	public FEWSSource(String filename)
	{
		this.setFilename(filename);
	}

	@Override
	public void saveForecast() throws IOException {
		PIXMLReader sourceReader = new PIXMLReader(this.getFilename(), true);
		sourceReader.setDataSourceConfig(this.getDataSourceConfig());
		sourceReader.setSpecifiedFeatures(this.getSpecifiedFeatures());
		sourceReader.parse();
	}

	@Override
	public void saveObservation() throws IOException {
		PIXMLReader sourceReader = new PIXMLReader(this.getAbsoluteFilename(), false);
		sourceReader.setDataSourceConfig(this.getDataSourceConfig());
		sourceReader.setSpecifiedFeatures(this.getSpecifiedFeatures());
		sourceReader.parse();
	}

}
