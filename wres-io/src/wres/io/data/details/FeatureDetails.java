package wres.io.data.details;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.CoordinateSelection;
import wres.config.generated.Feature;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;
import wres.util.Strings;

/**
 * Defines the important details of a feature as stored in the database
 * @author Christopher Tubbs
 */
public final class FeatureDetails extends CachedDetail<FeatureDetails, FeatureDetails.FeatureKey>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureDetails.class);

    // Prevents asynchronous saving of identical features
    private static final Object FEATURE_SAVE_LOCK = new Object();

	private String lid = null;
	private String featureName = null;
	private Integer featureId = null;
	private Integer comid = null;
	private String gageID = null;
	private String region = null;
	private String state = null;
	private String stateCode = null;
	private String huc = null;

    private Float longitude = null;
	private Float latitude = null;
	private Integer nwmIndex = null;

	private List<String> aliases = null;

	private FeatureKey key = null;
	
	// A concurrent mapping of the feature to its index for a variable
	private final ConcurrentMap<Integer, Integer> variablePositions = new ConcurrentHashMap<>();
	private static final Object POSITION_LOCK = new Object();

	public FeatureDetails()
    {
        super();
    }

    public FeatureDetails( FeatureKey key)
    {
        this.setLid( key.lid );
        this.setComid( key.comid );
        this.setHuc( key.huc );
        this.setGageID( key.gageID );
    }

    public FeatureDetails( ResultSet row) throws SQLException
    {
        this.update( row );
    }

    public Feature toFeature()
    {
        Long comID = null;

        if (this.getComid() != null)
        {
            comID = this.getComid().longValue();
        }

        CoordinateSelection coordinateSelection = null;

        if (this.longitude != null && this.latitude != null)
        {
            coordinateSelection = new CoordinateSelection(
                    this.longitude,
                    this.latitude,
                    0.0f );
        }

        return new Feature( aliases,
                            coordinateSelection,
                            null,
                            null,
                            this.getFeatureName(),
                            this.getLid(),
                            comID,
                            this.getGageID(),
                            this.getHuc(),
                            this.getFeatureName(),
                            this.getRegion(),
                            this.getState(),
                            null );
    }

    @Override
    protected void update( ResultSet row ) throws SQLException
    {
        if (Database.hasColumn( row, "comid" ))
        {
            this.setComid( Database.getValue( row, "comid" ) );
        }

        if (Database.hasColumn( row, "lid" ))
        {
            this.setLid( Database.getValue( row, "lid") );
        }

        if (Database.hasColumn( row, "gage_id" ))
        {
            this.setGageID( Database.getValue( row, "gage_id" ) );
        }

        if (Database.hasColumn( row, "region" ))
        {
            this.setRegion( Database.getValue( row, "region" ) );
        }

        if (Database.hasColumn( row, "st" ))
        {
            this.setState( Database.getValue(row, "st") );
        }

        if (Database.hasColumn( row, "huc" ))
        {
            this.setHuc( Database.getValue(row, "huc") );
        }

        if (Database.hasColumn( row, "feature_name" ))
        {
            this.setFeatureName( Database.getValue( row, "feature_name" ) );
        }

        if (Database.hasColumn( row, "latitude" ))
        {
            this.setLatitude( Database.getValue(row, "latitude") );
        }

        if (Database.hasColumn( row, "longitude" ))
        {
            this.setLongitude( Database.getValue( row, "longitude" ) );
        }

        if (Database.hasColumn( row, this.getIDName() ))
        {
            this.setID( Database.getValue( row, this.getIDName() ) );
        }
    }
	
	/**
	 * Retrieves the stored variable position id for the given variable
	 * @param variableID The ID of the variable to look for
	 * @return The id of the variable position mapping the feature to the
     * variable, null if an ID has not been added for the variable
	 */
	public Integer getVariableFeatureID(Integer variableID)
	{
		synchronized (POSITION_LOCK)
        {
            return this.variablePositions.get( variableID );
        }
	}

	public void addVariableFeature(int variableId, int variablefeatureId)
    {
        this.variablePositions.putIfAbsent( variableId, variablefeatureId );
    }
	
	/**
	 * Sets the LID of the Feature
	 * @param lid The value used to update the current LID with
	 */
	public void setLid( String lid)
	{
	    // Only set the value if you won't be erasing a value
		if (this.lid == null || ( Strings.hasValue( lid ) && !this.lid.equalsIgnoreCase(lid)))
		{
			this.lid = lid;

			// Reset the ID since a key changed
			this.featureId = null;
		}
	}

	public void setComid(Integer comid)
    {
        // Only set the value if you won't be erasing a value
        if (this.comid == null || (comid != null && !this.comid.equals(comid)))
        {
            this.comid = comid;

            // Reset the ID since a key changed
            this.featureId = null;
        }
    }

    public void setGageID(String gageID)
    {
        // Only set the value if you won't be erasing a value
        if (this.gageID == null || (Strings.hasValue( gageID ) && !this.gageID.equalsIgnoreCase( gageID )))
        {
            this.gageID = gageID;

            // Reset the ID since a key changed
            this.featureId = null;
        }
    }

    public void setHuc(String huc)
    {
        // Only set the value if you won't be erasing a value
        if (this.huc == null || (Strings.hasValue( huc ) && !this.huc.equalsIgnoreCase( huc )))
        {
            this.huc = huc;

            // Reset the ID since a key changed
            this.featureId = null;
        }
    }

    public void setAliases(List<String> aliases)
    {
        this.aliases = aliases;
    }

	@Override
	public int compareTo(FeatureDetails other) {
		Integer id = this.featureId;
		
		if (id == null)
		{
			id = -1;
		}

		return id.compareTo(other.featureId );
	}

	@Override
	public FeatureKey getKey()
    {
        if (this.key == null)
        {
            this.key = new FeatureKey( this.getComid(),
                                       this.getLid(),
                                       this.getGageID(),
                                       this.getHuc(),
                                       this.getLongitude(),
                                       this.getLatitude() );
        }
		return this.key;
	}

	@Override
	public Integer getId()
	{
		return this.featureId;
	}

	@Override
	protected String getIDName()
	{
		return "feature_id";
	}

	@Override
	public void setID(Integer id)
	{
		this.featureId = id;
	}

	public Integer getComid()
    {
        return this.comid;
    }

    public String getGageID()
    {
        return this.gageID;
    }

    public String getRegion()
    {
        return this.region;
    }

    public void setRegion( String region )
    {
        // Only set the value if you won't be erasing a value
        if ( this.region == null || Strings.hasValue ( region ))
        {
            this.region = region;
        }
    }

    public String getState()
    {
        return this.state;
    }

    public void setState(String state)
    {
        // Only set the value if you won't be erasing a value
        if (this.state == null || Strings.hasValue (state))
        {
            this.state = state;
        }
    }

    public String getHuc()
    {
        return this.huc;
    }

    public Integer getNwmIndex()
    {
        return nwmIndex;
    }

    public String getLid()
    {
        return lid;
    }

    public String getFeatureName()
    {
        return featureName;
    }

    public void setFeatureName(String featureName)
    {
        // Only set the value if you won't be erasing a value
        if (this.featureName == null || Strings.hasValue(featureName ))
        {
            this.featureName = featureName;
        }
    }

    public Float getLongitude()
    {
        return longitude;
    }

    public void setLongitude(Float longitude)
    {
        // Only set the value if you won't be erasing a value
        if (this.longitude == null || longitude != null)
        {
            this.longitude = longitude;
        }
    }

    public Float getLatitude()
    {
        return latitude;
    }

    public void setLatitude(Float latitude)
    {
        // Only set the value if you won't be erasing a value
        if (this.latitude == null || latitude != null)
        {
            this.latitude = latitude;
        }
    }

    @Override
    protected Logger getLogger()
    {
        return FeatureDetails.LOGGER;
    }

    @Override
    protected PreparedStatement getInsertSelectStatement( Connection connection)
            throws SQLException
    {
        List<Object> arguments = new ArrayList<>();
        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine( this.getInsert( arguments ) );
        script.addLine();
        script.addLine("UNION");
        script.addLine();
        script.add(this.getSelect( arguments ));

        return script.getPreparedStatement( connection, arguments );
    }

    private String getInsert(final List<Object> args)
    {
        ScriptBuilder script = new ScriptBuilder(  );

        // Keeps track of whether or not a field has been added.
        // When true, there needs to be a comma to separate the upcoming field
        // from the previous field, along with a newline
        boolean lineAdded = false;

        script.addLine( "WITH new_feature AS" );
        script.addLine("(");
        script.addTab().addLine("INSERT INTO wres.Feature (");

        if (this.getComid() != null)
        {
            script.addTab(  2  ).add("comid");

            // A field has been added
            lineAdded = true;
        }

        if (this.getLid() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script.addLine(",");
            }
            else
            {
                // A field has been added
                lineAdded = true;
            }

            script.addTab(2).add("lid");
        }

        if (this.getGageID() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script.addLine(",");
            }
            else
            {
                // A field has been added
                lineAdded = true;
            }
            script.addTab(2).add("gage_id");
        }

        if ( this.getRegion() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script.addLine(",");
            }
            else
            {
                // A field has been added
                lineAdded = true;
            }

            script.addTab(2).add("region");
        }

        if (this.getState() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script.addLine(",");
            }
            else
            {
                // A field has been added
                lineAdded = true;
            }

            script.addTab(2).add("state");
        }

        if (this.getHuc() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script.addLine(",");
            }
            else
            {
                // A field has been added
                lineAdded = true;
            }

            script.addTab(2).add("huc");
        }

        if (this.getFeatureName() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script.addLine(",");
            }
            else
            {
                // A field has been added
                lineAdded = true;
            }

            script.addTab(2).add("feature_name");
        }

        if (this.getLatitude() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script.addLine(",");
            }
            else
            {
                // A field has been added
                lineAdded = true;
            }

            script.addTab(2).add("latitude");
        }

        if (this.getLongitude() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script.addLine(",");
            }

            script.addTab(2).add("longitude");
        }

        script.addLine();
        script.addTab().addLine(")");

        // Now keeps track if we've added a line for a value
        // If true, we need to add a separator between encountered values
        lineAdded = false;

        script.addTab().addLine("SELECT");

        if (this.getComid() != null)
        {
            script.addTab(2).add("?");

            args.add(this.getComid());

            // A value has now been added
            lineAdded = true;
        }

        if (this.getLid() != null)
        {
            if (lineAdded)
            {
                // Separate this value from the previous one
                script.addLine(",");
            }
            else
            {
                // A value will be added
                lineAdded = true;
            }

            script.addTab(2).add("?");
            args.add( this.getLid() );
        }

        if (this.getGageID() != null)
        {
            if (lineAdded)
            {
                // Separate this value from the previous one
                script.addLine(",");
            }
            else
            {
                // A value will be added
                lineAdded = true;
            }


            script.addTab(2).add("?");
            args.add(this.getGageID());
        }

        if ( this.getRegion() != null)
        {
            if (lineAdded)
            {
                // Separate this value from the previous one
                script.addLine(",");
            }
            else
            {
                // A value will be added
                lineAdded = true;
            }

            script.addTab(2).add("?");
            args.add(this.getRegion());
        }

        if (this.getState() != null)
        {
            if (lineAdded)
            {
                // Separate this value from the previous one
                script.addLine(",");
            }
            else
            {
                // A value will be added
                lineAdded = true;
            }

            script.addTab(2).add("?");
            args.add(this.getState());
        }

        if (this.getHuc() != null)
        {
            if (lineAdded)
            {
                // Separate this value from the previous one
                script.addLine(",");
            }
            else
            {
                // A value will be added
                lineAdded = true;
            }

            script.addTab(2).add("?");
            args.add(this.getHuc());
        }

        if (this.getFeatureName() != null)
        {
            if (lineAdded)
            {
                // Seperate this value from the previous one
                script.addLine(",");
            }
            else
            {
                // A value will be added
                lineAdded = true;
            }

            script.addTab(2).add("?");
            args.add(this.getFeatureName());
        }

        if (this.getLatitude() != null)
        {
            if (lineAdded)
            {
                // Separate this value from the previous one
                script.addLine(",");
            }
            else
            {
                // A value will be added
                lineAdded = true;
            }

            script.addTab(2).add("?");
            args.add(this.getLatitude());
        }

        if (this.getLongitude() != null)
        {
            if (lineAdded)
            {
                // Separate this value from the previous one
                script.addLine(",");
            }

            script.addTab(2).add("?");
            args.add(this.getLongitude());
        }

        script.addLine();
        script.addTab().addLine("WHERE NOT EXISTS (");
        script.addTab(  2  ).addLine("SELECT 1");
        script.addTab(  2  ).addLine("FROM wres.Feature");
        script.addTab(  2  ).add("WHERE ");

        // Indicates whether or not a where clause has been added
        // If true, an 'OR' operator needs to be added
        lineAdded = false;

        if (this.getComid() != null)
        {
            script.add("comid = ?");
            args.add( this.getComid() );

            // A clause was added
            lineAdded = true;
        }

        if (Strings.hasValue( this.getLid() ))
        {
            if (lineAdded)
            {
                // Separate this clause from the previous with an OR operator
                script.addLine();
                script.addTab(3).add("OR ");
            }
            else
            {
                // A clause will be added
                lineAdded = true;
            }

            script.add("lid = ?");
            args.add( this.getLid() );
        }

        if (Strings.hasValue( this.getGageID() ))
        {
            if (lineAdded)
            {
                // Separate this clause from the previous with an OR operator
                script.addLine();
                script.addTab(3).add("OR ");
            }

            script.add("gage_id = ?");
            args.add(this.getGageID());
        }

        script.addLine();
        script.addTab().addLine(")");
        script.addTab().addLine("RETURNING feature_id,");
        script.addTab(  2  ).addLine("comid,");
        script.addTab(  2  ).addLine("lid,");
        script.addTab(  2  ).addLine("gage_id,");
        script.addTab(  2  ).addLine("region,");
        script.addTab(  2  ).addLine("state,");
        script.addTab(  2  ).addLine("huc,");
        script.addTab(  2  ).addLine("feature_name,");
        script.addTab(  2  ).addLine("latitude,");
        script.addTab(  2  ).addLine("longitude");
        script.addLine(")");
        script.addLine("SELECT *");
        script.add("FROM new_feature");

        return script.toString();
    }

    private String getSelect(final List<Object> args)
    {
        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine("SELECT feature_id,");
        script.addTab().addLine("comid,");
        script.addTab().addLine("lid,");
        script.addTab().addLine("gage_id,");
        script.addTab().addLine("region,");
        script.addTab().addLine("state,");
        script.addTab().addLine("huc,");
        script.addTab().addLine("feature_name,");
        script.addTab().addLine("latitude,");
        script.addTab().addLine("longitude");
        script.addLine("FROM wres.Feature");
        script.add("WHERE ");

        // Determines if an OR operator needs to be added to separate clauses
        boolean clauseAdded = false;

        if (this.getComid() != null)
        {
            script.add("comid = ?");
            args.add(this.getComid());

            // A clause was added
            clauseAdded = true;
        }

        if (Strings.hasValue( this.getLid() ))
        {
            if (clauseAdded)
            {
                // Separate clauses by an OR operator
                script.addLine();
                script.addTab().add("OR ");
            }
            else
            {
                // A clause will be added
                clauseAdded = true;
            }

            script.add("lid = ?");
            args.add(this.getLid());
        }

        if (Strings.hasValue( this.getGageID() ))
        {
            if (clauseAdded)
            {
                // Separate clauses by an OR operator
                script.addLine();
                script.addTab().add("OR ");
            }

            script.add("gage_id = ?");
            args.add(this.getGageID());
        }

        script.addLine();
        script.add("LIMIT 1;");

        return script.toString();
    }

    @Override
    protected Object getSaveLock()
    {
        return FeatureDetails.FEATURE_SAVE_LOCK;
    }

    @Override
    public String toString()
    {
        String name = "Unknown";

        if (Strings.hasValue( this.getLid() ))
        {
            name = this.getLid();
        }
        else if (Strings.hasValue( this.getFeatureName() ))
        {
            name = this.getFeatureName();
        }
        else if (Strings.hasValue( this.getGageID() ))
        {
            name = "Gage: " + this.getGageID();
        }
        else if (this.getComid() != null)
        {
            name = "NHDPlus ID: " + this.getComid();
        }
        else if (this.longitude != null && this.latitude != null)
        {
            name = "(" + this.longitude + ", " + this.latitude + ")";
        }

        return name;
    }

    public static FeatureKey keyOfLid(String lid)
    {
        return new FeatureKey( null, lid, null, null, null, null );
    }

    @Override
    public boolean equals( Object obj )
    {
        boolean equals = false;

        if (obj instanceof FeatureDetails)
        {
            FeatureDetails other = (FeatureDetails)obj;

            if (this.comid != null && other.comid != null)
            {
                equals = this.comid.equals( other.comid );
            }
            else if (Strings.hasValue(this.lid) && Strings.hasValue(other.lid))
            {
                equals = this.lid.equalsIgnoreCase( other.lid );
            }
            else if (Strings.hasValue( this.gageID ) && Strings.hasValue( other.gageID ))
            {
                equals = this.gageID.equalsIgnoreCase( other.gageID );
            }
            else if (this.latitude != null && this.longitude != null && other.latitude != null && other.longitude != null)
            {
                equals = this.latitude.equals( other.latitude ) && this.longitude.equals( other.longitude );
            }
        }

        return equals;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getLid(), this.getGageID(), this.getComid() );
    }

    public static class FeatureKey implements Comparable<FeatureKey>
    {
        private final Integer comid;
        private final String lid;
        private final String gageID;
        private final String huc;
        private final Float longitude;
        private final Float latitude;

        public FeatureKey (Integer comid, String lid, String gageID, String huc, Float longitude, Float latitude)
        {
            this.comid = comid;
            this.lid = lid;
            this.gageID = gageID;
            this.huc = huc;
            this.longitude = longitude;
            this.latitude = latitude;
        }

        @Override
        public String toString()
        {
            return "Comid: '" + String.valueOf(comid) +
                   "', lid: '" + String.valueOf(lid) +
                   "', gageID: '" + String.valueOf(gageID) +
                   "', huc: '" + String.valueOf(huc) +
                   "', Longitude: '" + String.valueOf(this.longitude) +
                   "', Latitude: '" + String.valueOf(this.latitude);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(this.comid, this.lid, this.gageID, this.huc);
        }

        @Override
        public boolean equals( Object obj )
        {
            Boolean equal = null;

            if (obj != null && obj instanceof FeatureKey)
            {
                FeatureKey other = (FeatureKey)obj;

                if (other.comid != null && other.comid > 0 &&
                    this.comid != null && this.comid > 0)
                {
                    equal = this.comid.equals( other.comid );
                }

                if ( equal == null && Strings.hasValue(other.lid) && Strings.hasValue( this.lid ))
                {
                    equal = other.lid.equalsIgnoreCase( this.lid );
                }

                if ( equal == null && Strings.hasValue( other.gageID ) && Strings.hasValue( this.gageID ))
                {
                    equal = other.gageID.equalsIgnoreCase( this.gageID );
                }

                if ( equal == null && Strings.hasValue( other.huc ) && Strings.hasValue( this.huc ))
                {
                    equal = other.huc.equalsIgnoreCase( this.huc );
                }

                if (equal == null && this.longitude != null && this.latitude != null &&
                    other.longitude != null && other.latitude != null)
                {
                    equal = this.longitude.equals( other.longitude ) && this.latitude.equals( other.latitude );
                }
            }

            if (equal == null)
            {
                equal = false;
            }

            return equal;
        }

        @Override
        public int compareTo( FeatureKey featureKey )
        {
            int comparison = 0;

            if (featureKey == null)
            {
                comparison = 1;
            }
            else if (this.equals( featureKey ))
            {
                comparison = 0;
            }
            else
            {
                if (Strings.hasValue( this.lid ) && Strings.hasValue( featureKey.lid ))
                {
                    comparison = this.lid.compareToIgnoreCase( featureKey.lid );
                }

                if (comparison == 0 && Strings.hasValue( this.gageID ) && Strings.hasValue( featureKey.gageID ))
                {
                    comparison = this.gageID.compareToIgnoreCase( featureKey.gageID );
                }

                if (comparison == 0 && this.comid != null && featureKey.comid != null)
                {
                    comparison = this.comid.compareTo( featureKey.comid );
                }

                if (comparison == 0 && this.longitude != null && featureKey.longitude != null)
                {
                    comparison = this.longitude.compareTo( featureKey.longitude );
                }

                if (comparison == 0 && this.latitude != null && featureKey.latitude != null)
                {
                    comparison = this.latitude.compareTo( featureKey.latitude );
                }
            }

            return comparison;
        }
    }
}
