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
public class Range extends ClauseConfig {
	
	public Range(XMLStreamReader reader) {
		super(reader);
	}

	@Override
	protected void interpret(XMLStreamReader reader) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected String tag_name() {
		// TODO Auto-generated method stub
		return null;
	}
	
	/* (non-Javadoc)
	 * @see config.data.Feature#get_condition()
	 */
	@Override
	public String get_condition(TreeMap<String, String> aliases) {
		boolean append_and = false;
		String condition = "";
		
		if (x_minimum != null)
		{
			condition += aliases.get("variableposition_alias") + ".x_position >= '" + x_minimum;
			append_and = true;
		}
		
		if (x_maximum != null)
		{
			if (append_and)
			{
				condition += " AND ";
			}
			condition += aliases.get("variableposition_alias") + ".x_position <= " + x_maximum;
			append_and = true;
		}
		
		if (y_minimum != null)
		{
			if (append_and)
			{
				condition += " AND ";
			}
			condition += aliases.get("variableposition_alias") + ".y_position >= " + y_minimum;
			append_and = true;
		}
		
		if (y_maximum != null)
		{
			if (append_and)
			{
				condition += " AND ";
			}
			condition += aliases.get("variableposition_alias") + ".y_position <= " + y_maximum;
		}
		
		if (!condition.isEmpty()){
			condition = "(" + condition + ")";
		}

		return condition;
	}
	
	public String x_minimum()
	{
		return x_minimum;
	}
	
	public String x_maximum()
	{
		return x_maximum;
	}
	
	public String y_minimum()
	{
		return y_minimum;
	}
	
	public String y_maximum()
	{
		return y_maximum;
	}

	private String x_minimum = null;
	private String x_maximum = null;
	private String y_minimum = null;
	private String y_maximum = null;
}
