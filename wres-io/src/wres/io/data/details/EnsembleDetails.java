package wres.io.data.details;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.EnsembleDetails.EnsembleKey;
import wres.io.utilities.ScriptBuilder;
import wres.util.Strings;

/**
 * Describes basic information used to define an Ensemble from the database
 * @author Christopher Tubbs
 */
public final class EnsembleDetails extends CachedDetail<EnsembleDetails, EnsembleKey>
{
    public EnsembleDetails (String name, String ensembleMemberID, String qualifierID)
    {
        this.ensembleName = name;
        this.ensembleMemberID = ensembleMemberID;
        this.qualifierID = qualifierID;
    }

    public EnsembleDetails()
    {
        super();
    }

    private static final Logger
            LOGGER = LoggerFactory.getLogger( EnsembleDetails.class);

    // Lock that will prevent the saving of the ensemble multiple times in a row
    private static final Object ENSEMBLE_SAVE_LOCK = new Object();
	
	// The name of the ensemble being represented
	private String ensembleName = null;
	
	// The "numeric" id of the member of the ensemble
	private String ensembleMemberID = null;
	
	// The serial id of the ensemble in the database
	private Integer ensembleID = null;

	public void setQualifierID( String qualifierID )
	{
		this.qualifierID = qualifierID;
	}

	// The qualifier for the ensemble
	private String qualifierID = null;
	
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
	 * @param ensembleMemberID The new id for the ensemble member
	 */
	public void setEnsembleMemberID(String ensembleMemberID)
	{
		if (this.ensembleMemberID == null || !this.ensembleMemberID.equalsIgnoreCase(ensembleMemberID))
		{
			this.ensembleMemberID = ensembleMemberID;
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
	private String getEnsembleMemberID()
	{
		String id = "0";
		
		if (ensembleMemberID != null)
		{
			id = this.ensembleMemberID;
		}
		
		return id;
	}

	@Override
	public int compareTo(EnsembleDetails other)
	{
		int comparison = this.ensembleName.compareToIgnoreCase(other.ensembleName);
		
		if (comparison == 0)
		{
			comparison = this.ensembleMemberID.compareTo(other.ensembleMemberID);
		}

		if (comparison == 0)
		{
			comparison = this.qualifierID.compareToIgnoreCase( other.qualifierID );
		}

		return comparison;
	}

	@Override
	public EnsembleKey getKey()
	{
		return new EnsembleKey(this.ensembleName, this.qualifierID, this.ensembleMemberID);
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
	protected PreparedStatement getInsertSelectStatement( Connection connection )
			throws SQLException
	{
		List<Object> args = new ArrayList<>(  );
		ScriptBuilder script = new ScriptBuilder(  );

		script.addLine("WITH new_ensemble AS");
		script.addLine("(");
		script.addTab().addLine("INSERT INTO wres.Ensemble(ensemble_name, qualifier_id, ensemblemember_id)");
		script.addTab().addLine("SELECT ?, ?, ?");

		args.add(this.ensembleName);
		args.add(this.getQualifierID());
		args.add(Integer.parseInt(this.getEnsembleMemberID()));

		script.addTab().addLine("WHERE NOT EXISTS (");
		script.addTab(  2  ).addLine("SELECT 1");
		script.addTab(  2  ).addLine("FROM wres.Ensemble");
		script.addTab(  2  ).addLine("WHERE ensemble_name = ?");

		args.add(this.ensembleName);

		script.addTab(   3   ).addLine("AND ensemblemember_id = ?");

		args.add(Integer.parseInt(this.getEnsembleMemberID()));

		script.addTab(   3   ).addLine("AND qualifier_id = ?");

		args.add(this.getQualifierID());

		script.addTab().addLine(")");
		script.addTab().addLine("RETURNING ensemble_id");
		script.addLine(")");
		script.addLine("SELECT ensemble_id");
		script.addLine("FROM new_ensemble");
		script.addLine();
		script.addLine("UNION");
		script.addLine();
		script.addLine("SELECT ensemble_id");
		script.addLine("FROM wres.Ensemble");
		script.addLine("WHERE ensemble_name = ?");

		args.add(this.ensembleName);

		script.addTab().addLine("AND ensemblemember_id = ?");

		args.add(Integer.parseInt(this.getEnsembleMemberID()));

		script.addTab().addLine("AND qualifier_id = ?;");

		args.add(this.getQualifierID());

		return script.getPreparedStatement( connection, args.toArray( new Object[args.size()] ) );
	}

	@Override
    protected Object getSaveLock()
    {
        return EnsembleDetails.ENSEMBLE_SAVE_LOCK;
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
               ", Member: " + this.getEnsembleMemberID() + "}";
    }

    public static EnsembleKey createKey(String ensembleName, String qualifierID, String memberIndex)
	{
	    return new EnsembleKey(ensembleName, qualifierID, memberIndex);
	}
	
	public static class EnsembleKey implements Comparable<EnsembleKey>
	{
	    EnsembleKey(String ensembleName, String qualifierID, String memberIndex)
	    {
	    	if (ensembleName == null)
			{
				this.ensembleName = "default";
			}
			else
			{
				this.ensembleName = ensembleName;
			}

			if (qualifierID == null)
            {
                this.qualifierID = "";
            }
            else
            {
                this.qualifierID = qualifierID;
            }

	    	if (memberIndex == null)
            {
                this.memberIndex = "0";
            }
            else
            {
                this.memberIndex = memberIndex;
            }
	    }

        @Override
        public int compareTo(EnsembleKey other)
        {
            int equality = this.getEnsembleName().compareTo( other.getEnsembleName() );

            if (equality == 0)
			{
				equality = this.getMemberIndex().compareTo( other.getMemberIndex() );
			}

			if (equality == 0)
			{
				equality = this.getQualifierID().compareTo( other.getQualifierID() );
			}

            return equality;
        }

        short fillCount()
        {
            short count = 0;

            if (!(this.getEnsembleName() == null || this.getEnsembleName().isEmpty()))
            {
                count++;
            }

            if (!(this.getMemberIndex() == null || this.getMemberIndex().isEmpty()))
            {
                count++;
            }

            if (!(this.getQualifierID() == null || this.getQualifierID().isEmpty()))
            {
                count++;
            }

            return count;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof EnsembleDetails.EnsembleKey && this.compareTo((EnsembleKey)obj) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.ensembleName, this.memberIndex, this.qualifierID);
        }

        String getEnsembleName()
        {
            return this.ensembleName;
        }
        
        String getQualifierID()
        {
            return this.qualifierID;
        }
        
        String getMemberIndex()
        {
            return this.memberIndex;
        }

        @Override
        public String toString() {
            return "EnsembleKey: " + this.ensembleName + ", " +
                    this.memberIndex + ", " +
                    this.qualifierID;
        }

        private final String ensembleName;
        private final String qualifierID;
        private final String memberIndex;
	}
}
