/**
 * 
 */
package config.data;

import java.util.TreeMap;

import javax.xml.stream.XMLStreamReader;

/**
 * @author ctubbs
 *
 */
public abstract class ClauseConfig extends ConfigElement{
	public ClauseConfig(XMLStreamReader reader) {
		super(reader);
	}

	public abstract String getCondition(TreeMap<String, String> aliases);
}
