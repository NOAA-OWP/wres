/**
 * 
 */
package data.definition;

import java.sql.ResultSet;
import java.sql.SQLException;

@Deprecated
/**
 * A definition for a variable in the public.Variable table
 * @author Christopher Tubbs
 * @deprecated All use of definitions for variables needs to be changed to use the VariableCache
 */
public class VariableDef
{
    /**
     * Constructs a variable based on the results from a select statement
     * @param result The ResultSet from a select statement
     * @throws SQLException Thrown when values cannot be retrieved from the result set
     */
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
	
	/**
	 * Constructor
	 */
	public VariableDef() {}
	
	/**
	 * Sets the <b>variable_name</b> of the variable
	 * @param name The value to update the current <b>variable_name</b> with
	 */
	public void set_name(String name)
	{
		this.variable_name = name;
	}
	
	/**
	 * Sets the <b>variable_type</b> of the variable
	 * @param type The value to update the current <b>variable_type</b> with
	 */
	public void set_type(String type)
	{
		this.variable_type = type;
	}
	
	/**
	 * Sets the <b>description</b> of the variable
	 * @param description The value to update the current <b>description</b> with. Corresponds to
	 * the long name of a variable
	 */
	public void set_description(String description)
	{
		this.description = description;
	}
	
	/**
	 * Sets the <b>measurementunit_id</b> of the variable
	 * @param measurementunit_id The value to update the current <b>measurementunit_id</b> with
	 */
	public void set_measurementunit_id(int measurementunit_id)
	{
		this.measurementunit_id = measurementunit_id;
	}
	
	/**
	 * @return The current id of the variable
	 */
	public int get_variable_id()
	{
		return variable_id;
	}
	
	/**
	 * @return The current name of the variable
	 */
	public String get_name()
	{
		return variable_name;
	}
	
	/**
	 * @return The current type of value stored with the variable
	 */
	public String get_type()
	{
		return variable_type;
	}
	
	/**
	 * @return The current long name for the variable
	 */
	public String get_description()
	{
		return description;
	}
	
	/**
	 * @return The ID of the unit that the variable is measured in
	 */
	public int get_measurementunit_id()
	{
		return measurementunit_id;
	}
	
	/**
	 * Verifies that the variable is suitable for use
	 * @throws Exception Thrown if the variable is deemed unacceptable
	 */
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