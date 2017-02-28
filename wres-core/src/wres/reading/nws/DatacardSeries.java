/**
 * 
 */
package wres.reading.nws;

import wres.reading.BasicSeries;

/**
 * @author ctubbs
 *
 */
public class DatacardSeries extends BasicSeries {

	/**
	 * 
	 */
	public DatacardSeries() {
		// TODO Auto-generated constructor stub
	}

	public String get_series_identifier()
	{
		return series_identifier;
	}
	
	public void set_series_identifier(String identifier)
	{
		series_identifier = identifier;
	}
	
	public int get_first_month()
	{
		return first_month;
	}
	
	public void set_first_month(int month)
	{
		first_month = month;
	}
	
	public void set_first_month(String month)
	{
		month = month.trim();
		set_first_month(Integer.parseInt(month));
	}
	
	public int get_first_year()
	{
		return first_year;
	}
	
	public void set_first_year(int year)
	{
		first_year = year;
	}
	
	public void set_first_year(String year)
	{
		year = year.trim();
		set_first_year(Integer.parseInt(year));
	}
	
	private String series_identifier = "";
	private int first_month = 0;
	private int first_year = 0;
}
