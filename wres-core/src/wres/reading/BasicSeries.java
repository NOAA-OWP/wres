/**
 * 
 */
package wres.reading;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

/**
 * @author ctubbs
 *
 */
public abstract class BasicSeries implements Comparable<BasicSeries> {

	/**
	 * 
	 */
	public BasicSeries() {
		// TODO Auto-generated constructor stub
	}
	
	public void print()
	{
		System.out.println(toString());

		
		for (BasicSeriesEntry entry : data_entries)
		{
			entry.print();
		}
		
	}
	
	public String toString()
	{
		String message = "";
		
		if (get_start_date() != null)
		{
			message += "Start Date:\t";
			message += get_start_date().toString();
			message += System.lineSeparator();
		}
		
		if (get_end_date() != null)
		{
			message += "End Date:\t";
			message += get_end_date().toString();
			message += System.lineSeparator();
		}
		
		if (get_forecast_date() != null)
		{
			message += "Forecast Date:\t";
			message += get_forecast_date().toString();
			message += System.lineSeparator();
		}
		
		if (!ensemble_member_id.isEmpty())
		{
			message += "Ensemble Member ID:\t";
			message += ensemble_member_id;
			message += System.lineSeparator();
		}
		
		if (aggregation_period != 0.0)
		{
			message += "Aggregation Period:\t";
			message += String.valueOf(aggregation_period);
			message += System.lineSeparator();
		}
		
		if (temporal_statistic != wres.util.Statistic.UNKNOWN)
		{
			message += "Temporal Statistic:\t";
			message += temporal_statistic.toString();
			message += System.lineSeparator();
		}
		
		if (unit != wres.util.MeasurementUnit.UNKNOWN)
		{
			message += "Measurement Unit:\t";
			message += unit.toString();
			message += System.lineSeparator();
		}
		
		if (temporal_unit != wres.util.TemporalUnit.UNDEFINED)
		{
			message += "Temporal Unit:\t";
			message += temporal_unit.toString();
			message += System.lineSeparator();
		}
		
		if (unit_multiplier != 1.0)
		{
			message += "Unit Multiplier:\t";
			message += String.valueOf(unit_multiplier);
			message += System.lineSeparator();
		}
		
		if (!message.isEmpty())
		{
			message += System.lineSeparator();
		}
		
		message += "Values:";
		message += System.lineSeparator();
		
		return message;
	}
	
	public Iterator<BasicSeriesEntry> get_entries()
	{
		return data_entries.iterator();
	}
	
	public wres.util.Statistic get_temporal_statistic()
	{
		return temporal_statistic;
	}
	
	public void set_temporal_statistic(wres.util.Statistic statistic)
	{
		temporal_statistic = statistic;
	}
	
	public wres.util.TemporalUnit get_temporal_unit()
	{
		return temporal_unit;
	}
	
	public void set_temporal_unit(wres.util.TemporalUnit unit)
	{
		temporal_unit = unit;
	}
	
	public wres.util.MeasurementUnit get_unit()
	{
		return unit;
	}
	
	public void set_unit(wres.util.MeasurementUnit unit_of_measurement)
	{
		this.unit = unit_of_measurement;
	}
	
	public double get_no_data_value()
	{
		return no_data_value;
	}
	
	public void set_no_data_value(double value)
	{
		no_data_value = value;
	}
	
	public double get_unit_multiplier()
	{
		return unit_multiplier;
	}
	
	public void set_unit_multiplier(double multiplier)
	{
		unit_multiplier = multiplier;
	}
	
	public String get_ensemble_member_id()
	{
		return ensemble_member_id;
	}
	
	public void set_ensemble_member_id(String id)
	{
		ensemble_member_id = id;
	}
	
	public void add_entry(BasicSeriesEntry entry)
	{
		data_entries.add(entry);
	}
	
	public Date get_end_date()
	{
		return end_date;
	}
	
	public Date get_start_date()
	{
		return start_date;
	}
	
	public void set_start_date(Date started_at)
	{
		start_date = (Date)started_at.clone();
	}
	
	public void set_end_date(Date ended_at)
	{
		end_date = (Date)ended_at.clone();
	}
	
	public void set_forecast_date(Date predicted_at)
	{
		forecast_date = (Date)predicted_at.clone();
	}
	
	public Date get_forecast_date()
	{
		return forecast_date;
	}
	
	public double get_aggregation_period()
	{
		return aggregation_period;
	}
	
	public void set_aggregation_period(double period)
	{
		aggregation_period = period;
	}
	
	public int length()
	{
		return data_entries.size();
	}
	
	public String get_long_name()
	{
		return long_name;
	}
	
	public void set_long_name(String name)
	{
		long_name = name;
	}
	
	public String get_short_name()
	{
		return short_name;
	}
	
	public void set_short_name(String name)
	{
		short_name = name;
	}
	
	public int get_rank()
	{
		return rank;
	}
	
	public void set_rank(int depth)
	{
		rank = depth;
	}
	
	public wres.util.DataType get_data_type()
	{
		return data_type;
	}
	
	public void set_data_type(wres.util.DataType type)
	{
		data_type = type;
	}
	
	public void set_data_type(String typename)
	{
		data_type = wres.util.DataType.valueOf(typename);
	}
	
	public double get_minimum()
	{
		return minimum;
	}
	
	public void set_minimum(double min)
	{
		minimum = min;
	}
	
	public void set_minimum(String min)
	{
		minimum = Double.valueOf(min);
	}
	
	public double get_maximum()
	{
		return maximum;
	}
	
	public void set_maximum(double max)
	{
		maximum = max;
	}
	
	public void set_maximum(String max)
	{
		maximum = Double.valueOf(max);
	}
	
	public int get_modelvariable_id()
	{
		return modelvariable_id;
	}
	
	public void set_modelvariable_id(int id)
	{
		modelvariable_id = id;
	}

	private int modelvariable_id;
	private int rank = 0;
	private String long_name = "";
	private String short_name = "";
	private Date start_date = null;
	private Date end_date = null;
	private Date forecast_date = null;
	protected List<BasicSeriesEntry> data_entries = new Vector<BasicSeriesEntry>();
	private String ensemble_member_id = "";
	private double unit_multiplier = 1.0;
	private wres.util.Statistic temporal_statistic = wres.util.Statistic.UNKNOWN;
	private wres.util.MeasurementUnit unit = wres.util.MeasurementUnit.UNKNOWN;
	private wres.util.TemporalUnit temporal_unit = wres.util.TemporalUnit.UNDEFINED;
	private wres.util.DataType data_type = wres.util.DataType.DOUBLE;
	private double aggregation_period = 0.0;	
	private double minimum = -9999999999.0; // Explicitly defined since the Double minimum definition is positive
	private double maximum = Double.MAX_VALUE;
	
	private double no_data_value = -999.0;

	@Override
	public int compareTo(BasicSeries arg0) {
		// TODO Auto-generated method stub
		return 0;
	}
}
