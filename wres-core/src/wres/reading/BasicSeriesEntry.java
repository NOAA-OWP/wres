/**
 * 
 */
package wres.reading;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import wres.util.RealCollection;

/**
 * @author ctubbs
 *
 */
public abstract class BasicSeriesEntry implements Comparable<BasicSeriesEntry> {
	public RealCollection values = new RealCollection();
	public Date date;
	public double lead_time = 0.0;	
	
	public void print()
	{
		System.out.println(toString());
	}
	
	public void save(int modelvariable_id)
	{
		String entry_script = get_save_variablevalue_script();
		String value_script = "";
		Connection connection;
		try {
			connection = wres.util.Utilities.create_connection();
			Statement query = connection.createStatement();
			for (double value : values)
			{
				value_script = String.format(entry_script,
						 modelvariable_id,
						 wres.util.Utilities.convert_date_to_string(date),
						 lead_time,
						 value);
				query.execute(value_script);
			}
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private String get_save_variablevalue_script()
	{
		String script = "INSERT INTO ModelVariableValue(modelvariable_id,";
		script += System.lineSeparator();
		script += "\tvalue_date,";
		script += System.lineSeparator();
		script += "\tlead_time,";
		script += System.lineSeparator();
		script += "\tvariable_value)";
		script += System.lineSeparator();
		
		script += "VALUES(";
		script += System.lineSeparator();
		script += "\t%d,";								// ModelVariable ID
		script += System.lineSeparator();
		script += "\t'%s',";							// Value Date
		script += System.lineSeparator();
		script += "\t%f,";								// Lead Time
		script += System.lineSeparator();
		script += "\t%f)";								// Value
		script += System.lineSeparator();
		script += "RETURNING modelvariable_id;";
		
		return script;
	}
	
	public String toString()
	{
		String message = "";
		if (date != null)
		{
			message = date.toString();
			message += " ";
		}
		
		message += String.valueOf(lead_time);
		
		for (Double value : values)
		{
			message += " ";
			message += String.valueOf(value);
		}
		
		return message;
	}
	
	public void add_value(int value)
	{
		values.add((double)value);
	}
	
	public void add_value(float value)
	{
		values.add((double)value);
	}
	
	public void add_value(byte value)
	{
		values.add((double)value);
	}
	
	public void add_value(short value)
	{
		values.add((double)value);
	}
	
	public void add_value(long value)
	{
		values.add((double)value);
	}
	
	public void add_value(double value)
	{
		values.add(value);
	}
	
	public void add_value(String value)
	{
		values.add(Double.valueOf(value.trim()));
	}

	@Override
	public int compareTo(BasicSeriesEntry o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
