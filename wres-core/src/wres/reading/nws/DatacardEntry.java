/**
 * 
 */
package wres.reading.nws;

import wres.reading.BasicSeriesEntry;

/**
 * @author ctubbs
 *
 */
public class DatacardEntry extends BasicSeriesEntry {

	/**
	 * 
	 */
	public DatacardEntry() {
		// TODO Auto-generated constructor stub
	}

	public int get_sequence_number()
	{
		return sequence_number;
	}
	
	public void set_sequence_number(int number)
	{
		sequence_number = number;
	}
	
	public void set_sequence_number(String number)
	{
		number = number.trim();
		set_sequence_number(Integer.parseInt(number));
	}
	
	public String get_series_identifier()
	{
		return series_identifier;
	}
	
	public void set_series_identifier(String identifier)
	{
		series_identifier = identifier.trim();
	}
	
	private String series_identifier = "";
	private int sequence_number = 0;
}
