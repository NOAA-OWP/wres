/**
 * 
 */
package wres.reading.misc;

import java.util.Calendar;
import java.util.Date;

/**
 * @author ctubbs
 *
 */
public class ASCIISeries extends wres.reading.BasicSeries {

	/**
	 * 
	 */
	public ASCIISeries() {
		set_temporal_unit(wres.util.TemporalUnit.HOUR);
	}
	
	@Override
	public Date get_end_date()
	{
		if (data_entries.size() > 0)
		{
			return data_entries.get(data_entries.size() - 1).date;
		}
		else
		{
			return null;
		}
	}
	
	@Override
	public Date get_start_date()
	{
		if (data_entries.size() > 0)
		{
			return data_entries.get(0).date;
		}
		else
		{
			return null;
		}
	}
	
	@Override
	public Date get_forecast_date()
	{
		Date start = (Date)get_start_date().clone();
		
		if (start != null)
		{
			Calendar cal = Calendar.getInstance();
			cal.setTime(start);
			cal.add(Calendar.HOUR, (int)get_aggregation_period() * -1);
			start = cal.getTime();
		}
		
		return start;
	}
	

	@Override
	@Deprecated
	public void set_start_date(Date started_at)
	{
		if (data_entries.size() > 0)
		{
			data_entries.get(0).date = (Date)started_at.clone();
		}
	}
	
	@Override
	@Deprecated
	public void set_end_date(Date ended_at)
	{
		if (data_entries.size() > 0)
		{
			data_entries.get(data_entries.size() - 1).date = (Date)ended_at.clone();
		}
	}
}
