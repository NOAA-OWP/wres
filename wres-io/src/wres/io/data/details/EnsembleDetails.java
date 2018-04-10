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

/**
 * Describes basic information used to define an Ensemble from the database
 * @author Christopher Tubbs
 */
public final class EnsembleDetails extends CachedDetail<EnsembleDetails, EnsembleKey>
{
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
			id = "'" + qualifierID + "'";
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
		int comparison = this.ensembleName.compareTo(other.ensembleName);
		
		if (comparison == 0)
		{
			comparison = this.ensembleMemberID.compareTo(other.ensembleMemberID);
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

	        this.qualifierID = qualifierID;

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
            int equality;

            if (!this.hasName() && !other.hasName())
            {
                equality = 0;
            }
            else if (!this.hasName() && other.hasName())
            {
                equality = -1;
            }
            else if ( this.hasName() && !other.hasName())
			{
				equality = 1;
			}
            else
            {
                equality = this.ensembleName.compareTo(other.getEnsembleName());
            }

            if (equality == 0 && (this.hasMemberIndex() || other.hasMemberIndex()))
            {
            	if (!this.hasMemberIndex() && other.hasMemberIndex())
				{
					equality = -1;
				}
				else if (this.hasMemberIndex() && !other.hasMemberIndex())
				{
					equality = 1;
				}
				else
				{
					equality =
							this.memberIndex.compareTo( other.getMemberIndex() );
				}
            }

            if (equality == 0 && !(this.qualifierIsMissing() && other.qualifierIsMissing()))
            {
            	if (this.qualifierIsMissing())
				{
					equality = -1;
				}
				else if (other.qualifierIsMissing())
				{
					equality = 1;
				}
				else
				{
					equality =
							this.qualifierID.compareTo( other.getQualifierID() );
				}
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

        public String getEnsembleName()
        {
            return this.ensembleName;
        }
        
        public String getQualifierID()
        {
            return this.qualifierID;
        }
        
        public String getMemberIndex()
        {
            return this.memberIndex;
        }

        private boolean hasName()
        {
            return !(this.getEnsembleName().equals( "default" ) || this.getEnsembleName().isEmpty());
        }

        private boolean hasMemberIndex()
        {
            return !(this.getMemberIndex().equals( "0" ) || this.getMemberIndex().isEmpty());
        }

        private boolean qualifierIsMissing()
		{
			return this.getQualifierID() == null || this.getQualifierID().isEmpty();
		}

        /**
         * Returns the degree of similarity (0-3) between this key and the other.
         * The similarity is 1 if they both have the same name
         * The similarity is 2 if they both have the same name and member index
         * The similarity is 3 if they both have the same name, member index, and qualifier
         * The similarity is 0 in all other cases
         * @param other The other key to compare against
         * @return The degree of similarity
         */
        public Integer getSimilarity(EnsembleKey other)
        {
        	// If the other is non-existent or more fields exist in this key
			// than the other, we know that this is not a subset of the other
			// and the similarity needs to be invalidated
            if (other == null || this.fillCount() > other.fillCount())
            {
                return 0;
            }
            // If the two keys are equal, we know that there is a perfect
			// similarity
            else if (this.equals(other))
            {
                return 3;
            }

            // If the names are the same, we know that there is some similarity
			// between the two
            if ((this.hasName() && other.hasName()) && this.getEnsembleName().equalsIgnoreCase(other.getEnsembleName()))
            {
            	// If this doesn't have a member index or both are null,
				// we know that the similarity ends with the name
                if(!this.hasMemberIndex() || !(this.hasMemberIndex() || other.hasMemberIndex()))
                {
                    return 1;
                }
                // If this has a member index and the other doesn't or they both
				// do and they are different, we know that this is not a subset
				// of the other
                else if ((this.hasMemberIndex() && !other.hasMemberIndex()) ||
                        !this.getMemberIndex().equalsIgnoreCase(other.getMemberIndex()))
                {
                    return 0;
                }
				// If neither of these have a qualifier or this doesn't have a
				// qualifier, we know that the similarity is two because we have
				// passed the testing on member indices.
                else if ((this.qualifierIsMissing() && other.qualifierIsMissing()) || this.qualifierIsMissing())
                {
                    return 2;
                }
            }

            return 0;
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
