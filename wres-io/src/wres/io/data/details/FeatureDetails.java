package wres.io.data.details;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import wres.config.generated.Feature;
import wres.io.utilities.Database;
import wres.util.Internal;
import wres.util.Strings;

/**
 * Defines the important details of a feature as stored in the database
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public final class FeatureDetails extends CachedDetail<FeatureDetails, FeatureDetails.FeatureKey>
{
	private String lid = null;
	private String featureName = null;
	private Integer featureId = null;
	private Integer comid = null;
	private String gageID = null;
	private String rfc = null;
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
        Long comid = null;

        if (this.getComid() != null)
        {
            comid = this.getComid().longValue();
        }

        return new Feature( aliases,
                            null,
                            this.getFeatureName(),
                            this.getLid(),
                            comid,
                            this.getGageID(),
                            this.getHuc(),
                            this.getFeatureName(),
                            this.getRfc(),
                            null );
    }

    private void update(ResultSet row) throws SQLException
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

        if (Database.hasColumn( row, "rfc" ))
        {
            this.setRfc( Database.getValue( row, "rfc" ) );
        }

        if (Database.hasColumn( row, "st" ))
        {
            this.setState( Database.getValue(row, "st") );
        }

        if (Database.hasColumn( row, "st_code" ))
        {
            this.setStateCode( Database.getValue( row, "st_code" ) );
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

        if (Database.hasColumn( row, "nwm_index" ))
        {
            this.nwmIndex = Database.getValue( row, "nwm_index" );
        }

        if (Database.hasColumn( row, this.getIDName() ))
        {
            this.setID( Database.getValue( row, this.getIDName() ) );
        }
    }
	
	/**
	 * Finds the variable position id of the feature for a given variable. A position is
	 * added if there is not one for this pair of variable and feature
	 * @param variableID The ID of the variable to look for
	 * @return The id of the variable position mapping the feature to the variable
	 * @throws SQLException if the attempt to find the position fails
	 */
	public Integer getVariablePositionID(Integer variableID) throws SQLException
	{
		Integer id;

		synchronized (POSITION_LOCK)
        {
			if (!variablePositions.containsKey(variableID))
			{
                String script = "";

                script += "WITH new_variableposition_id AS" + NEWLINE;
                script += "(" + NEWLINE;
                script += "		INSERT INTO wres.VariablePosition (variable_id, x_position)" + NEWLINE;
                script += "		SELECT " + String.valueOf(variableID) + ", " + String.valueOf(getId()) + NEWLINE;
                script += "		WHERE NOT EXISTS (" + NEWLINE;
                script += "			SELECT 1" + NEWLINE;
                script += "			FROM wres.VariablePosition VP" + NEWLINE;
                script += "			WHERE VP.variable_id = " + String.valueOf(variableID) + NEWLINE;
                script += "				AND VP.x_position = " + String.valueOf(getId()) + NEWLINE;
                script += "		)" + NEWLINE;
                script += "		RETURNING variableposition_id" + NEWLINE;
                script += ")" + NEWLINE;
                script += "SELECT variableposition_id" + NEWLINE;
                script += "FROM new_variableposition_id" + NEWLINE + NEWLINE;
                script += "";
                script += "UNION" + NEWLINE + NEWLINE;
                script += "";
                script += "SELECT variableposition_id" + NEWLINE;
                script += "FROM wres.VariablePosition VP" + NEWLINE;
                script += "WHERE VP.variable_id = " + String.valueOf(variableID) + NEWLINE;
                script += "		AND VP.x_position = " + String.valueOf(getId()) + ";";

				Integer dbResult = Database.getResult(script, "variableposition_id");

				if (dbResult == null)
				{
					// TODO: throw an appropriate checked exception instead of RuntimeException
					throw new RuntimeException("Possibly missing data in the wres.variableposition table?");
				}
				variablePositions.putIfAbsent(variableID, dbResult);
			}

			id = variablePositions.get(variableID);
		}
		
		return id;
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
                                       this.getHuc() );
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

    public String getRfc()
    {
        return this.rfc;
    }

    public void setRfc(String rfc)
    {
        // Only set the value if you won't be erasing a value
        if (this.rfc == null || Strings.hasValue (rfc))
        {
            this.rfc = rfc;
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

    public String getStateCode()
    {
        return this.stateCode;
    }

    public void setStateCode(String stateCode)
    {
        // Only set the value if you won't be erasing a value
        if (this.stateCode == null || Strings.hasValue (stateCode))
        {
            this.stateCode = stateCode;
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
    public void save() throws SQLException
    {
        //This needs to populate fields if they are missing values

        Connection connection = null;
        ResultSet feature = null;

        try
        {
            connection = Database.getHighPriorityConnection();
            feature = Database.getResults( connection, this.getInsertSelectStatement() );

            while (feature.next())
            {
                this.update( feature );
            }
        }
        finally
        {
            if (feature != null)
            {
                feature.close();
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection( connection );
            }
        }
    }

    @Override
	protected String getInsertSelectStatement()
    {
		String script = "";

		script += this.getInsertStatement();

		script += NEWLINE;
		script += "UNION" + NEWLINE;
		script += NEWLINE;

		script += this.getSelectStatement();

		return script;
	}

    /**
     * Creates the 'Insert' portion of the InsertSelectStatement that will
     * create and retrieve a new record based on the current fields
     * @return The 'Insert' portion of the InsertSelectStatement
     */
	private String getInsertStatement()
    {
        // Keeps track of whether or not a field has been added.
        // When true, there needs to be a comma to separate the upcoming field
        // from the previous field, along with a newline
        boolean lineAdded = false;
        String script = "";

        script += "WITH new_feature AS" + NEWLINE;
        script += "(" + NEWLINE;
        script += "    INSERT INTO wres.Feature (" + NEWLINE;

        if (this.getComid() != null)
        {
            script += "        comid";

            // A field has been added
            lineAdded = true;
        }

        if (this.getLid() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script += "," + NEWLINE;
            }
            else
            {
                // A field has been added
                lineAdded = true;
            }

            script += "        lid," + NEWLINE;
            script += "        parent_feature_id";
        }

        if (this.getGageID() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script += "," + NEWLINE;
            }
            else
            {
                // A field has been added
                lineAdded = true;
            }

            script += "        gage_id";
        }

        if (this.getRfc() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script += "," + NEWLINE;
            }
            else
            {
                // A field has been added
                lineAdded = true;
            }

            script += "        rfc";
        }

        if (this.getState() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script += "," + NEWLINE;
            }
            else
            {
                // A field has been added
                lineAdded = true;
            }

            script += "        st";
        }

        if (this.getStateCode() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script += "," + NEWLINE;
            }
            else
            {
                // A field has been added
                lineAdded = true;
            }

            script += "        st_code";
        }

        if (this.getHuc() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script += "," + NEWLINE;
            }
            else
            {
                // A field has been added
                lineAdded = true;
            }

            script += "        huc";
        }

        if (this.getFeatureName() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script += "," + NEWLINE;
            }
            else
            {
                // A field has been added
                lineAdded = true;
            }

            script += "        feature_name";
        }

        if (this.getLatitude() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script += "," + NEWLINE;
            }
            else
            {
                // A field has been added
                lineAdded = true;
            }

            script += "        latitude";
        }

        if (this.getLongitude() != null)
        {
            if (lineAdded)
            {
                // Add a separator between this and the previously added field
                script += "," + NEWLINE;
            }

            script += "        longitude";
        }

        script += NEWLINE;
        script += "    )" + NEWLINE;

        // Now keeps track if we've added a line for a value
        // If true, we need to add a separator between encountered values
        lineAdded = false;

        script += "    SELECT" + NEWLINE;

        if (this.getComid() != null)
        {
            script += "        " + String.valueOf(this.getComid());

            // A value has now been added
            lineAdded = true;
        }

        if (this.getLid() != null)
        {
            if (lineAdded)
            {
                // Seperate this value from the previous one
                script += "," + NEWLINE;
            }
            else
            {
                // A value will be added
                lineAdded = true;
            }

            script += "        '" + String.valueOf( this.getLid() ) + "'," + NEWLINE;

            script += "        (" + NEWLINE;
            script += "            SELECT feature_id" + NEWLINE;
            script += "            FROM wres.Feature F" + NEWLINE;
            script += "            WHERE '" + this.lid + "' LIKE F.lid || '%'" + NEWLINE;
            script += "        )";
        }

        if (this.getGageID() != null)
        {
            if (lineAdded)
            {
                // Seperate this value from the previous one
                script += "," + NEWLINE;
            }
            else
            {
                // A value will be added
                lineAdded = true;
            }

            script += "        '" + String.valueOf( this.getGageID() ) + "'";
        }

        if (this.getRfc() != null)
        {
            if (lineAdded)
            {
                // Seperate this value from the previous one
                script += "," + NEWLINE;
            }
            else
            {
                // A value will be added
                lineAdded = true;
            }

            script += "        '" + String.valueOf( this.getRfc() ) + "'";
        }

        if (this.getState() != null)
        {
            if (lineAdded)
            {
                // Seperate this value from the previous one
                script += "," + NEWLINE;
            }
            else
            {
                // A value will be added
                lineAdded = true;
            }

            script += "        '" + String.valueOf( this.getState() ) + "'";
        }

        if (this.getStateCode() != null)
        {
            if (lineAdded)
            {
                // Seperate this value from the previous one
                script += "," + NEWLINE;
            }
            else
            {
                // A value will be added
                lineAdded = true;
            }

            script += "        '" + String.valueOf( this.getStateCode() ) + "'";
        }

        if (this.getHuc() != null)
        {
            if (lineAdded)
            {
                // Seperate this value from the previous one
                script += "," + NEWLINE;
            }
            else
            {
                // A value will be added
                lineAdded = true;
            }

            script += "        '" + String.valueOf( this.getHuc() ) + "'";
        }

        if (this.getFeatureName() != null)
        {
            if (lineAdded)
            {
                // Seperate this value from the previous one
                script += "," + NEWLINE;
            }
            else
            {
                // A value will be added
                lineAdded = true;
            }

            script +=
                    "        '" + String.valueOf( this.getFeatureName() ) + "'";
        }

        if (this.getLatitude() != null)
        {
            if (lineAdded)
            {
                // Seperate this value from the previous one
                script += "," + NEWLINE;
            }
            else
            {
                // A value will be added
                lineAdded = true;
            }

            script += "        " + String.valueOf( this.getLatitude() );
        }

        if (this.getLongitude() != null)
        {
            if (lineAdded)
            {
                // Seperate this value from the previous one
                script += "," + NEWLINE;
            }

            script += "        " + String.valueOf( this.getLongitude() );
        }

        script += NEWLINE;
        script += "    WHERE NOT EXISTS (" + NEWLINE;
        script += "        SELECT 1" + NEWLINE;
        script += "        FROM wres.Feature" + NEWLINE;

        // Indicates whether or not a where clause has been added
        // If true, an 'OR' operator needs to be added
        lineAdded = false;

        script += "        WHERE ";

        if (this.getComid() != null)
        {
            script += "comid = " + String.valueOf(this.getComid());

            // A clause was added
            lineAdded = true;
        }

        if (Strings.hasValue( this.getLid() ))
        {
            if (lineAdded)
            {
                // Separate this clause from the previous with an OR operator
                script += NEWLINE;
                script += "            OR ";
            }
            else
            {
                // A clause will be added
                lineAdded = true;
            }

            script += "lid = '" + String.valueOf(this.getLid()) + "'";
        }

        if (Strings.hasValue( this.getGageID() ))
        {
            if (lineAdded)
            {
                // Separate this clause from the previous with an OR operator
                script += NEWLINE;
                script += "            OR ";
            }

            script += "gage_id = '" + String.valueOf(this.getGageID()) + "'";
        }

        script += NEWLINE;
        script += "    )" + NEWLINE;
        script += "    RETURNING feature_id, comid, lid, gage_id, rfc, st, st_code, huc, feature_name, latitude, longitude, nwm_index" + NEWLINE;
        script += ")" + NEWLINE;
        script += "SELECT feature_id, comid, lid, gage_id, rfc, st, st_code, huc, feature_name, latitude, longitude, nwm_index" + NEWLINE;
        script += "FROM new_feature" + NEWLINE;

        return script;
    }

    /**
     * Creates the 'Select' portion of the InsertSelectStatement that will
     * retrieve preexisting values matching the current keys
     * @return The 'Select' portion of the InsertSelectStatement
     */
    private String getSelectStatement()
    {
        String script = "";

        script += "SELECT feature_id, comid, lid, gage_id, rfc, st, st_code, huc, feature_name, latitude, longitude, nwm_index" + NEWLINE;
        script += "FROM wres.feature" + NEWLINE;

        script += "WHERE ";

        // Determines if an OR operator needs to be added to separate clauses
        boolean clauseAdded = false;

        if (this.getComid() != null)
        {
            script += "comid = " + String.valueOf(this.getComid());

            // A clause was added
            clauseAdded = true;
        }

        if (Strings.hasValue( this.getLid() ))
        {
            if (clauseAdded)
            {
                // Separate clauses by an OR operator
                script += NEWLINE;
                script += "    OR ";
            }
            else
            {
                // A clause will be added
                clauseAdded = true;
            }

            script += "lid = '" + String.valueOf(this.getLid()) + "'" + NEWLINE;
        }

        if (Strings.hasValue( this.getGageID() ))
        {
            if (clauseAdded)
            {
                // Separate clauses by an OR operator
                script += NEWLINE;
                script += "    OR ";
            }

            script += "gage_id = '" + String.valueOf(this.getGageID()) + "'" + NEWLINE;
        }

        script += "LIMIT 1;";

        return script;
    }

    @Override
    public String toString()
    {
        String name = "Unknown";

        if (Strings.hasValue( this.getFeatureName() ))
        {
            name = this.getFeatureName();
        }
        else if (Strings.hasValue( this.getLid() ))
        {
            name = this.getLid();
        }
        else if (Strings.hasValue( this.getGageID() ))
        {
            name = "Gage: " + this.getGageID();
        }

        return name;
    }

    public static FeatureKey keyOfComid( Integer comid)
    {
        return new FeatureKey( comid, null, null, null );
    }

    public static FeatureKey keyOfLid(String lid)
    {
        return new FeatureKey( null, lid, null, null );
    }

    public static FeatureKey keyOfGageID(String gageID)
    {
        return new FeatureKey( null, null, gageID, null );
    }

    public static FeatureKey keyOfHuc(String huc)
    {
        return new FeatureKey( null, null, null, huc );
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

        public FeatureKey (Integer comid, String lid, String gageID, String huc)
        {
            this.comid = comid;
            this.lid = lid;
            this.gageID = gageID;
            this.huc = huc;
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
                    comparison = this.lid.compareTo( featureKey.lid );
                }

                if (comparison == 0 && Strings.hasValue( this.gageID ) && Strings.hasValue( featureKey.gageID ))
                {
                    comparison = this.gageID.compareTo( featureKey.gageID );
                }

                if (comparison == 0 && this.comid != null && featureKey.comid != null)
                {
                    comparison = this.comid.compareTo( featureKey.comid );
                }
            }

            return comparison;
        }
    }
}
