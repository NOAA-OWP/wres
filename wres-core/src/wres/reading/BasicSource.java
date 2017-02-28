/**
 * 
 */
package wres.reading;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import wres.reading.misc.ASCIISource;
import wres.reading.nws.DatacardSource;

/**
 * @author ctubbs
 *
 */
public abstract class BasicSource {
	
	public abstract void read() throws Exception;
	public abstract void read_and_save() throws Exception;
	public abstract void write(String path);
	public abstract void save_forecast() throws SQLException;
	public abstract void save_observation() throws Exception;
	
	public static void write(BasicSource source, String path)
	{
		source.write(path);
	}
	
	public final static void write(SourceType type, BasicSource source, String path)
	{
		switch(type)
		{
		case DATACARD:
			DatacardSource.write(source, path);
		case ASCII:
			ASCIISource.write(source, path);
			break;
		default:
			write(source, path);
		}
	}
	
	public void print()
	{
		System.out.println(toString());
		
		for (BasicSeries series : time_series)
		{
			series.print();
			System.out.println("");
		}		
	}
	
	public void save_source() throws SQLException
	{
		Connection db_connection = wres.util.Utilities.create_connection();
		String save_script = String.format(get_save_model_script(), 
											wres.util.Utilities.convert_date_to_string(get_forecast_date()), 
										 get_filename(), 
										 get_location_id());
		Statement query = db_connection.createStatement();
		query.execute(save_script);
		ResultSet results = query.getResultSet();
		results.next();
		set_model_id(results.getInt(1));
		results.close();
		db_connection.close();
	}
	
	public void save() throws SQLException
	{
		Connection db_connection = wres.util.Utilities.create_connection();
		String model_insert = "";
		String variable_script = "";
		String value_script = "";
		
		try
		{
			db_connection.setAutoCommit(false);
			Statement query = db_connection.createStatement();
			model_insert = get_save_model_script();
			model_insert = String.format(model_insert, 
										 wres.util.Utilities.convert_date_to_string(get_forecast_date()), 
										 get_filename(), 
										 get_location_id());
			
			if (query.execute(model_insert))
			{
				ResultSet results = query.getResultSet();
				results.next();
				int model_id = results.getInt(1);
				for (BasicSeries series : time_series)
				{
					variable_script = get_save_variable_script();
					variable_script = String.format(variable_script,
													model_id,
													series.get_short_name(),
													series.get_long_name(),
													series.get_no_data_value(),
													series.get_rank(),
													series.get_data_type().toString(),
													series.get_aggregation_period(),
													series.get_minimum(),
													series.get_maximum(),
													series.get_unit().toString(),
													get_location_id(),
													series.get_ensemble_member_id());
					
					query = db_connection.createStatement();
					if(query.execute(variable_script))
					{
						results = query.getResultSet();
						results.next();
						int modelvariable_id = results.getInt(1);
						
						Iterator<BasicSeriesEntry> entries = series.get_entries();
						int entry_count = 0;
						while (entries.hasNext() && entry_count < 80)
						{
							BasicSeriesEntry entry = entries.next();
							String entry_script = get_save_variablevalue_script();
							for (Double value : entry.values)
							{
								value_script = String.format(entry_script,
															 modelvariable_id,
															 wres.util.Utilities.convert_date_to_string(entry.date),
															 entry.lead_time,
															 value);
								query.execute(value_script);
							}
							entry_count++;
						}
					}
				}
			}
		}
		catch (Exception error)
		{
			db_connection.rollback();
			db_connection.close();

			System.out.println("An error occured while saving data:");
			if (!model_insert.isEmpty())
			{
				System.out.println("");
				System.out.println("The model insert script was:");
				System.out.println("");
				System.out.println(model_insert);
			}
			
			if (!variable_script.isEmpty())
			{
				System.out.println("");
				System.out.println("The variable insert script was:");
				System.out.println("");
				System.out.println(variable_script);
			}
			
			if (!value_script.isEmpty())
			{
				System.out.println("");
				System.out.println("The value insert script was:");
				System.out.println("");
				System.out.println(value_script);
			}
			
			System.out.println("");
			
			throw error;
		}
		
		db_connection.commit();
	}
	
	protected String get_save_model_script()
	{
		String script = "INSERT INTO Model(valid_time, filename, title)";
		script += System.lineSeparator();
		script += "VALUES ('%s', '%s', '%s')";
		script += System.lineSeparator();
		script += "RETURNING model_id;";
		return script;
	}
	
	protected String get_save_variable_script()
	{
		String script = "INSERT INTO ModelVariable(model_id,";
		script += System.lineSeparator();
		script += "\tshort_name,";
		script += System.lineSeparator();
		script += "\tlong_name,";
		script += System.lineSeparator();
		script += "\tfill_value,";
		script += System.lineSeparator();
		script += "\trank,";
		script += System.lineSeparator();
		script += "\tdata_type,";
		script += System.lineSeparator();
		script += "\ttimestep,";
		script += System.lineSeparator();
		script += "\tmin,";
		script += System.lineSeparator();
		script += "\tmax,";
		script += System.lineSeparator();
		script += "\tunit,";
		script += System.lineSeparator();
		script += "\tlocation_name,";
		script += System.lineSeparator();
		script += "\tensemble_member_id)";
		script += System.lineSeparator();
		
		script += "VALUES (";
		script += System.lineSeparator();
		script += "\t%d,";								// Model ID
		script += System.lineSeparator();
		script += "\t'%s',";							// Short Name
		script += System.lineSeparator();
		script += "\t'%s',";							// Long name
		script += System.lineSeparator();
		script += "\t%f,";								// Fill Value
		script += System.lineSeparator();
		script += "\t%d,";								// Rank
		script += System.lineSeparator();
		script += "\t'%s',";							// Data Type
		script += System.lineSeparator();
		script += "\t%f,";								// Time Step
		script += System.lineSeparator();
		script += "\t%f,";								// minimum
		script += System.lineSeparator();
		script += "\t%f,";								// maximum
		script += System.lineSeparator();
		script += "\t'%s',";							// unit
		script += System.lineSeparator();
		script += "\t'%s',";							// location name
		script += System.lineSeparator();
		script += "\t'%s'";								// Ensemble Member ID
		script += System.lineSeparator();
		script += ")";
		script += System.lineSeparator();
		script += "RETURNING modelvariable_id;";
		
		return script;
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
	
	@Override
 	public String toString()
	{
		String message = "";
		
		message += "Filename:\t";
		message += filename;
		message += System.lineSeparator();
		
		if (time_zone != wres.util.TimeZone.UNDEFINED)
		{
			message += "Time Zone:\t";
			message += time_zone.toString();
			message += System.lineSeparator();
		}
		
		if (!location_id.isEmpty())
		{
			message += "Location:\t";
			message += location_id;
			message += System.lineSeparator();
		}
		
		if (!ensemble_id.isEmpty())
		{
			message += "Ensemble:\t";
			message += ensemble_id;
			message += System.lineSeparator();
		}
		
		if (!variable_id.isEmpty())
		{
			message += "Variable:\t";
			message += variable_id;
			message += System.lineSeparator();
		}
		
		if (source_type != SourceType.UNDEFINED)
		{
			message += "Type:\t";
			message += source_type.toString();
			message += System.lineSeparator();
		}
		
		message += System.lineSeparator();
		
		message += "Series:";
		message += System.lineSeparator();
		
		return message;
	}

	public String get_filename()
	{
		return filename;
	}
	
	public wres.util.TimeZone get_time_zone()
	{
		return time_zone;
	}
	
	protected void set_time_zone(String offset)
	{
		time_zone = wres.util.TimeZone.valueOf(offset);
	}
	
	protected void set_filename(String name)
	{
		filename = name;
	}
	
	public String get_notes()
	{
		return notes;
	}
	
	public void set_notes(String note)
	{
		notes = note;
	}
	
	public SourceType get_source_type()
	{
		return source_type;
	}
	
	protected void set_source_type(SourceType type)
	{
		source_type = type;
	}
	
	public String get_location_id()
	{
		return location_id;
	}
	
	protected void set_location_id(String id)
	{
		location_id = id;
	}
	
	public String get_variable_id()
	{
		return variable_id;
	}
	
	protected void set_variable_id(String id)
	{
		variable_id = id;
	}
	
	public String get_ensemble_id()
	{
		return ensemble_id;
	}
	
	public void set_ensemble_id(String id)
	{
		ensemble_id = id;
	}
	
	public Iterator<BasicSeries> get_series()
	{
		return time_series.iterator();
	}
	
	private String filename = "";
	private wres.util.TimeZone time_zone = wres.util.TimeZone.UNDEFINED;
	
	protected Date get_forecast_date()
	{
		return forecast_date;
	}
	
	protected void set_forecast_date(Date forecasted_date)
	{
		forecast_date = (Date)forecasted_date.clone();
	}
	
	protected int get_model_id()
	{
		return model_id;
	}
	
	protected void set_model_id(int id)
	{
		model_id = id;
	}
	
	protected void set_model_id(String id)
	{
		model_id = Integer.parseInt(id.trim());
	}
	
	private int model_id;
	protected ArrayList<BasicSeries> time_series = new ArrayList<BasicSeries>();
	private Date forecast_date = null;
	private String notes = "";
	private String location_id = "";
	private String variable_id = "";
	private String ensemble_id = "";
	private SourceType source_type = SourceType.UNDEFINED;
}
