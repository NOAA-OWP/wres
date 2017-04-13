/**
 * 
 */
package config.data;

import java.util.ArrayList;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * @author ctubbs
 *
 */
public class MetricData extends ClauseConfig {

	/**
	 * @param reader
	 */
	public MetricData(XMLStreamReader reader) {
		super(reader);
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see config.data.ClauseConfig#get_condition(java.lang.String, java.lang.String)
	 */
	@Override
	public String get_condition(TreeMap<String, String> aliases) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see config.data.ConfigElement#interpret(javax.xml.stream.XMLStreamReader)
	 */
	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see config.data.ConfigElement#tag_name()
	 */
	@Override
	protected String tag_name() {
		// TODO Auto-generated method stub
		return null;
	}

	private Conditions conditions;
	private Aggregation aggregation;
	private String variable = null;
	private ArrayList<ClauseConfig> features = new ArrayList<ClauseConfig>();
	private boolean all_ensembles = false;
	private ArrayList<Ensemble> ensembles;
}
