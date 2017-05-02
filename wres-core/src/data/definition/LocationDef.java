/**
 * 
 */
package data.definition;

@Deprecated
/**
 * A definition for data used and retrieved from the public.ObservationLocation table
 * @author Christopher Tubbs
 * @deprecated Location information needs to be used from the FeatureCache rather than the old
 * location definitions
 */
public class LocationDef {

	/**
	 * Constructor
	 */
	public LocationDef() {}
	
	/**
	 * Constructor
	 * <br>
	 * Constructs the location based on latitude and longitude
	 * <br><br>
	 * Defaults the location datum to <b>NGVD29</b>
	 * @param latitude The initial value for the latitude of the location 
	 * @param longitude The initial value for the longitude of the location
	 */
	public LocationDef(float latitude, float longitude)
	{
		set_latitude(latitude);
		set_longitude(longitude);
		set_datum("NGVD29");
	}
	
	/**
	 * 
	 * @param feature_id
	 */
	public LocationDef(int feature_id)
	{
		set_feature_id(feature_id);
	}
	
	/**
	 * Verifies that the location is valid for in-memory operations
	 * @throws Exception An exception is thrown if the location is deemed unacceptable for use
	 */
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
	
	/**
	 * @return True if the definition is to be used to identify a location based on longitude and latitude
	 */
	public boolean for_coordinates()
	{
		return this.latitude != null && this.longitude != null;
	}
	
	/**
	 * @return True if the definition is to be used to identify a location based on its ID
	 */
	public boolean for_feature()
	{
		return this.feature_id != null;
	}
	
	/**
	 * @return The <b>comid</b> of the location. -1 is returned if it is currently null.
	 */
	public int get_feature_id()
	{
		return (this.feature_id != null) ? this.feature_id : -1;
	}
	
	/**
	 * Sets the <b>comid</b> of the location
	 * @param feature_id The value to update the current <b>comid</b> with
	 */
	public void set_feature_id(Integer feature_id)
	{
		this.feature_id = feature_id;
	}
	
	/**
	 * @return The current value of the location's <b>latitude</b>. 0.0 is returned if it is
	 * currently null.
	 */
	public Float get_latitude()
	{
		return (this.latitude != null) ? this.latitude : 0.0F;
	}
	
	/**
	 * Sets the <b>latitude</b> of the location
	 * @param latitude The value to update the current <b>latitude</b> with
	 */
	public void set_latitude(Float latitude)
	{
		this.latitude = latitude;
	}
	
	/**
	 * @return The current <b>longitude</b> of the location. 0.0 is returned if it is
	 * currently null.
	 */
	public Float get_longitude()
	{
		return (this.longitude != null) ? this.longitude : 0.0f;
	}
	
	/**
	 * Sets the <b>longitude</b> of the location
	 * @param longitude The value to update the current value of the <b>longitude</b> with.
	 */
	public void set_longitude(Float longitude)
	{
		this.longitude = longitude;
	}
	
	/**
	 * @return The current <b>datum</b> of the location. A blank string is returned if it is currently null
	 */
	public String get_datum()
	{
		return (this.datum != null) ? this.datum : "";
	}
	
	/**
	 * Sets the datum of the location
	 * @param datum The value to update the current <b>datum</b> with
	 */
	public void set_datum(String datum)
	{
		this.datum = datum;
	}
	
	/**
	 * @return The current <b>LID</b> of the location
	 */
	public String get_location_id()
	{
		return (this.location_id != null) ? this.location_id : "";
	}
	
	/**
	 * Sets the <b>LID</b> of the location
	 * @param location_id The value to update the current <b>LID</b> with
	 */
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
	
	/**
	 * @return The script used the insert a new location based on specified value.
	 */
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
