/**
 * 
 */
package data.definition;

/**
 * @author ctubbs
 *
 */
public class LocationDef {

	/**
	 * 
	 */
	public LocationDef() {}
	
	public LocationDef(float latitude, float longitude)
	{
		set_latitude(latitude);
		set_longitude(longitude);
		set_datum("NGVD29");
	}
	
	public LocationDef(int feature_id)
	{
		set_feature_id(feature_id);
	}
	
	public void validate() throws Exception
	{
		if (!for_coordinates() || !for_feature())
		{
			String message = "This location definition lacks the information needed to be valid" +
							 System.lineSeparator() +
							 toString();
			
			throw new Exception(message);
		}
	}
	
	public boolean for_coordinates()
	{
		return this.latitude != null && this.longitude != null;
	}
	
	public boolean for_feature()
	{
		return this.feature_id != null;
	}
	
	public Integer get_feature_id()
	{
		return (this.feature_id != null) ? this.feature_id : -1;
	}
	
	public void set_feature_id(Integer feature_id)
	{
		this.feature_id = feature_id;
	}
	
	public float get_latitude()
	{
		return (this.latitude != null) ? this.latitude : 0.0f;
	}
	
	public void set_latitude(float latitude)
	{
		this.latitude = latitude;
	}
	
	public float get_longitude()
	{
		return (this.longitude != null) ? this.longitude : 0.0f;
	}
	
	public void set_longitude(float longitude)
	{
		this.longitude = longitude;
	}
	
	public String get_datum()
	{
		return (this.datum != null) ? this.datum : "";
	}
	
	public void set_datum(String datum)
	{
		this.datum = datum;
	}
	
	public String get_location_id()
	{
		return (this.location_id != null) ? this.location_id : "";
	}
	
	public void set_location_id(String location_id)
	{
		this.location_id = location_id;
	}
	
	@Override
	public String toString()
	{
		String message = "Location Definition: " + System.lineSeparator()
						+ "		LID: '" + get_location_id() + "'" + System.lineSeparator()
						+ "		COMID: '" + get_feature_id() + "'" + System.lineSeparator()
						+ "		LATITUDE: '" + String.valueOf(get_latitude()) + "'" + System.lineSeparator()
						+ "		LONGITUDE: '" + String.valueOf(get_longitude()) + "'" + System.lineSeparator()
						+ "		DATUM: '" + get_datum() + "'" + System.lineSeparator()
						+ System.lineSeparator();
		
		return message;
	}
	
	public String add_script()
	{
		String script = "INSERT INTO ObservationLocation (\n" +
						"	comid,\n"
						+ "	lid,\n"
						+ "	gage_id,\n"
						+ "	st,\n"
						+ "	nws_st,\n"
						+ "	nws_lat,\n"
						+ "	nws_lon\n"
						+ ")\n"
						+ "VALUES (\n"
						+ "	" + get_feature_id() + ",\n"
						+ " '" + get_location_id() + "',\n"
						+ " '',\n"
						+ " '',\n"
						+ " '',\n"
						+ " " + get_latitude() + ",\n"
						+ " " + get_longitude() + "\n"
						+ ")";
		return script;
	}
	
	private Integer feature_id;
	private String location_id;
	private Float latitude;
	private Float longitude;
	private String datum;
}
