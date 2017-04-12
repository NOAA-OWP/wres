/**
 * 
 */
package reading.fews;

import reading.BasicSource;
import reading.XMLReader;

/**
 * @author ctubbs
 *
 */
public class FEWSSource extends BasicSource {

	/**
	 * 
	 */
	public FEWSSource() {}
	
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
