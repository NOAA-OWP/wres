/**
 * 
 */
package data.definition;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author ctubbs
 *
 */
public class VariableDef
{
	public VariableDef(ResultSet result) throws SQLException
	{
		if (result.isBeforeFirst())
		{
			result.next();
		}
		
		this.variable_id = result.getInt("variable_id");
		this.variable_name = result.getString("variable_name");
		this.variable_type = result.getString("variable_type");
		this.description = result.getString("description");
		this.measurementunit_id = result.getInt("measurementunit_id");
	}
	
	public VariableDef() {}
	
	public void set_name(String name)
	{
		this.variable_name = name;
	}
	
	public void set_type(String type)
	{
		this.variable_type = type;
	}
	
	public void set_description(String description)
	{
		this.description = description;
	}
	
	public void set_measurementunit_id(int measurementunit_id)
	{
		this.measurementunit_id = measurementunit_id;
	}
	
	public int get_variable_id()
	{
		return variable_id;
	}
	
	public String get_name()
	{
		return variable_name;
	}
	
	public String get_type()
	{
		return variable_type;
	}
	
	public String get_description()
	{
		return description;
	}
	
	public int get_measurementunit_id()
	{
		return measurementunit_id;
	}
	
	public void validate() throws Exception
	{
		if (this.variable_name == null || this.variable_name == "")
		{
			String message = "This variable definition lacks a name, and is thus invalid." +
							 System.lineSeparator() +
							 toString();
			
			throw new Exception(message);
		}
	}
	
	@Override
	public String toString()
	{
		String representation = "Variable Defintion:" + System.lineSeparator()
								+ "		ID: '" + String.valueOf(get_variable_id()) + "'" + System.lineSeparator()
								+ "		NAME: '" + get_name() + "'" + System.lineSeparator();
		
		return representation;
	}
	
	private int variable_id;
	private String variable_name;
	private String variable_type;
	private String description;
	private int measurementunit_id;
}