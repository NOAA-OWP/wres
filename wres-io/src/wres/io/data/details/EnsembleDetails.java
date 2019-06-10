package wres.io.data.details;

import java.sql.SQLException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.EnsembleDetails.EnsembleKey;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;

/**
 * Describes basic information used to define an Ensemble from the database
 * @author Christopher Tubbs
 */
public final class EnsembleDetails extends CachedDetail<EnsembleDetails, EnsembleKey>
{
    public EnsembleDetails ( String name, Integer ensembleMemberIndex, String qualifierID)
    {
        this.ensembleName = name;
        this.ensembleMemberIndex = ensembleMemberIndex;
        this.qualifierID = qualifierID;
    }

    public EnsembleDetails()
    {
        super();
    }

    private static final Logger
            LOGGER = LoggerFactory.getLogger( EnsembleDetails.class);
	
	/**
	 The name of the ensemble being represented
	  */
	private String ensembleName = null;

    /**
     * The index of the member of the ensemble
     */
	private Integer ensembleMemberIndex = null;
	
	/**
	 * The serial id of the ensemble in the database
     */
	private Integer ensembleID = null;

    /**
     * The qualifier for the ensemble
     */
    private String qualifierID = null;

	public void setQualifierID( String qualifierID )
	{
		this.qualifierID = qualifierID;
	}
	
	/**
	 * Updates the ensemble name if necessary. If the update occurs, the serial id is reset
	 * @param ensembleName The new name for the ensemble
	 */
	public void setEnsembleName(String ensembleName)
	{
		if (this.ensembleName == null || !this.ensembleName.equalsIgnoreCase(ensembleName))
		{
			this.ensembleName = ensembleName;
			this.ensembleID = null;
		}
	}
	
	/**
	 * Updates the ensemble member id if necessary. If the update occurs, the serial id is reset
	 * @param ensembleMemberIndex The new id for the ensemble member
	 */
	public void setEnsembleMemberIndex( final Integer ensembleMemberIndex )
	{
		if ( this.ensembleMemberIndex == null || !this.ensembleMemberIndex.equals( ensembleMemberIndex ))
		{
			this.ensembleMemberIndex = ensembleMemberIndex;
			this.ensembleID = null;
		}
	}
	
	/**
	 * Returns the proper string representation of the qualifier ID of the ensemble
	 * @return The qualifier id wrapped in single quotes or the unwrapped String stating null
	 */
	private String getQualifierID()
	{
		String id;
		
		if (qualifierID == null)
		{
			id = "null";
		}
		else
		{
			id = qualifierID;
		}
		
		return id;
	}
	
	/**
	 * Returns a sanitized version of the ensemblemember id for saving and loading values
	 * @return The id of the ensemble member if one exists, "0" otherwise
	 */
	private Integer getEnsembleMemberIndex()
	{
	    return this.ensembleMemberIndex;
	}

	@Override
	public int compareTo(EnsembleDetails other)
	{
	    int comparison = 0;

        if (this.ensembleName != null && other.ensembleName != null)
        {
            comparison = this.ensembleName.compareTo( other.ensembleName );
        }
        else if (this.ensembleName != null)
        {
            comparison = 1;
        }
        else if (other.ensembleName != null)
        {
            comparison = -1;
        }
		
		if (comparison == 0)
		{
            if ( this.getEnsembleMemberIndex() == null && other.getEnsembleMemberIndex() == null)
            {
                comparison = 0;
            }
            else if ( this.getEnsembleMemberIndex() != null && other.getEnsembleMemberIndex() != null)
            {
                comparison = this.getEnsembleMemberIndex().compareTo( other.getEnsembleMemberIndex() );
            }
            else if ( this.getEnsembleMemberIndex() != null)
            {
                comparison = 1;
            }
            else
            {
                comparison = -1;
            }
		}

		if (comparison == 0)
		{
            if (this.getQualifierID() == null && other.getQualifierID() == null)
            {
                comparison = 0;
            }
            else if (this.getQualifierID() != null && other.getQualifierID() != null)
            {
                comparison = this.getQualifierID().compareTo( other.getQualifierID() );
            }
            else if (this.getQualifierID() != null)
            {
                comparison = 1;
            }
            else
            {
                comparison = -1;
            }
		}

		return comparison;
	}

	@Override
	public EnsembleKey getKey()
	{
		return new EnsembleKey(this.ensembleName, this.qualifierID, this.ensembleMemberIndex );
	}

	@Override
	public Integer getId() {
		return this.ensembleID;
	}

	@Override
	protected String getIDName() {
		return "ensemble_id";
	}

	@Override
	public void setID(Integer id) {
		this.ensembleID = id;
	}

	@Override
	protected DataScripter getInsertSelect()
	{
        DataScripter script = new DataScripter();
        script.setUseTransaction( true );
        script.retryOnSqlState( "40001" );
        script.retryOnSqlState( "23505" );
        script.setHighPriority( true );

		script.addTab().addLine("INSERT INTO wres.Ensemble(ensemble_name, qualifier_id, ensemblemember_id)");
		script.addTab().addLine("SELECT ?, ?, ?");

		script.addArgument( this.ensembleName );

		if (this.qualifierID == null)
		{
			script.addArgument( null );
		}
		else
		{
		    script.addArgument( this.getQualifierID() );
		}

		if ( this.ensembleMemberIndex == null)
		{
		    script.addArgument( null );
		}
		else
		{
			script.addArgument( this.getEnsembleMemberIndex() );
		}

		script.addTab().addLine("WHERE NOT EXISTS (");
		script.addTab(  2  ).addLine("SELECT 1");
		script.addTab(  2  ).addLine("FROM wres.Ensemble");
		script.addTab(  2  ).addLine("WHERE ensemble_name = ?");

        script.addArgument(this.ensembleName);

		script.addTab(   3   ).add("AND ensemblemember_id ");

        if ( this.ensembleMemberIndex == null)
        {
            script.addLine("IS NULL");
        }
        else
        {
            script.addLine("= ?");
            script.addArgument( this.getEnsembleMemberIndex() );
        }

		script.addTab(   3   ).add("AND qualifier_id ");

        if (this.qualifierID == null)
        {
            script.addLine("IS NULL");
        }
        else
        {
            script.addLine("= ?");
            script.addArgument( this.getQualifierID() );
        }

		script.addTab().addLine(")");

        return script;
	}

    @Override
    public void save() throws SQLException
    {
        DataScripter script = this.getInsertSelect();
        boolean performedInsert = script.execute() > 0;

        if ( performedInsert )
        {
            this.ensembleID = script.getInsertedIds()
                                    .get( 0 )
                                    .intValue();
        }
        else
        {
            DataScripter scriptWithId = new DataScripter();
            scriptWithId.setHighPriority( true );
            scriptWithId.setUseTransaction( false );
            scriptWithId.addLine( "SELECT ensemble_id" );
            scriptWithId.addLine( "FROM wres.Ensemble" );
            scriptWithId.addLine( "WHERE ensemble_name = ?" );

            scriptWithId.addArgument( this.ensembleName );

            scriptWithId.addTab().add( "AND ensemblemember_id " );

            if ( this.ensembleMemberIndex == null )
            {
                scriptWithId.addLine( "IS NULL" );
            }
            else
            {
                scriptWithId.addLine( "= ?" );
                scriptWithId.addArgument( this.getEnsembleMemberIndex() );
            }

            scriptWithId.addTab().add( "AND qualifier_id " );

            if ( this.qualifierID == null )
            {
                scriptWithId.add( "IS NULL;" );
            }
            else
            {
                scriptWithId.add( "= ?;" );
                scriptWithId.addArgument( this.getQualifierID() );
            }

            try ( DataProvider data = scriptWithId.getData() )
            {
                this.ensembleID = data.getInt( this.getIDName() );
            }
        }

        LOGGER.trace( "Did I create Ensemble ID {}? {}",
                      this.ensembleID,
                      performedInsert );
    }

	@Override
    protected Object getSaveLock()
    {
        return new Object();
    }

    @Override
    protected Logger getLogger()
    {
        return EnsembleDetails.LOGGER;
    }

    @Override
    public String toString()
    {
        return "Ensemble: { ID: " + this.ensembleName +
               ", Qualifier: " + this.getQualifierID() +
               ", Member: " + this.getEnsembleMemberIndex() + "}";
    }

	public static class EnsembleKey implements Comparable<EnsembleKey>
	{
        private final String ensembleName;
        private final String qualifierID;
        private final Integer memberIndex;

	    EnsembleKey(String ensembleName, String qualifierID, Integer memberIndex)
	    {
            this.ensembleName = Objects.requireNonNullElse( ensembleName, "default" );
            this.qualifierID = Objects.requireNonNullElse( qualifierID, "" );
            this.memberIndex = Objects.requireNonNullElse( memberIndex, 0 );
	    }

        @Override
        public int compareTo(EnsembleKey other)
        {
            int equality = this.getEnsembleName().compareTo( other.getEnsembleName() );

            if (equality == 0)
			{
			    if (this.getMemberIndex() == null && other.getMemberIndex() == null)
                {
                    equality = 0;
                }
			    else if (this.getMemberIndex() != null && other.getMemberIndex() != null)
                {
                    equality = this.getMemberIndex().compareTo( other.getMemberIndex() );
                }
                else if (this.getMemberIndex() != null)
                {
                    equality = 1;
                }
                else
                {
                    equality = -1;
                }
			}

			if (equality == 0)
			{
				equality = this.getQualifierID().compareTo( other.getQualifierID() );
			}

            return equality;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof EnsembleDetails.EnsembleKey && this.compareTo((EnsembleKey)obj) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.ensembleName, this.memberIndex, this.qualifierID);
        }

        public String getEnsembleName()
        {
            return this.ensembleName;
        }
        
        public String getQualifierID()
        {
            return this.qualifierID;
        }
        
        public Integer getMemberIndex()
        {
            return this.memberIndex;
        }

        @Override
        public String toString() {
            return "EnsembleKey: " + this.ensembleName + ", " +
                    this.memberIndex + ", " +
                    this.qualifierID;
        }
	}
}
