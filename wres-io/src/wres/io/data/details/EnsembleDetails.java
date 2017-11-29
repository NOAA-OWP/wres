package wres.io.data.details;

import java.util.Objects;

import wres.io.data.details.EnsembleDetails.EnsembleKey;
import wres.util.Internal;

/**
 * Describes basic information used to define an Ensemble from the database
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public final class EnsembleDetails extends CachedDetail<EnsembleDetails, EnsembleKey>{
	
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
	 * @param ensemble_name The new name for the ensemble
	 */
	public void setEnsembleName(String ensemble_name)
	{
		if (this.ensembleName == null || !this.ensembleName.equalsIgnoreCase(ensemble_name))
		{
			this.ensembleName = ensemble_name;
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
	protected String getInsertSelectStatement() {
		String script = "";
		
		script += "WITH new_ensemble AS" + NEWLINE;
		script += "(" + NEWLINE;
		script += "		INSERT INTO wres.Ensemble(ensemble_name, qualifier_id, ensemblemember_id)" + NEWLINE;
		script += "		SELECT '" + ensembleName + "', " + getQualifierID() + ", " + getEnsembleMemberID() + NEWLINE;
		script += "		WHERE NOT EXISTS (" + NEWLINE;
		script += "			SELECT 1" + NEWLINE;
		script += "			FROM wres.Ensemble" + NEWLINE;
		script += "			WHERE ensemble_name = '" + ensembleName + "'" + NEWLINE;
		script += "				AND ensemblemember_id = " + getEnsembleMemberID() + NEWLINE;
		script += "		)" + NEWLINE;
		script += "		RETURNING ensemble_id" + NEWLINE;
		script += ")" + NEWLINE;
		script += "SELECT ensemble_id" + NEWLINE;
		script += "FROM new_ensemble" + NEWLINE + NEWLINE;
		script += "";
		script += "UNION" + NEWLINE + NEWLINE;
		script += "";
		script += "SELECT ensemble_id" + NEWLINE;
		script += "FROM wres.Ensemble" + NEWLINE;
		script += "WHERE ensemble_name = '" + ensembleName + "'" + NEWLINE;
		script += "		AND ensemblemember_id = " + getEnsembleMemberID() + ";";
		return script;
	}
	
	public static EnsembleKey createKey(String ensembleName, String qualifierID, String memberIndex)
	{
	    return new EnsembleKey(ensembleName, qualifierID, memberIndex);
	}
	
	public static class EnsembleKey implements Comparable<EnsembleKey>
	{
	    public EnsembleKey(String ensembleName, String qualifierID, String memberIndex)
	    {
	        this.ensembleName = ensembleName;
	        this.qualifierID = qualifierID;
	        this.memberIndex = memberIndex;
	    }

        @Override
        public int compareTo(EnsembleKey other)
        {
            int equality;

            if (this.ensembleName == null && other.ensembleName == null)
            {
                equality = 0;
            }
            else if (this.ensembleName == null && other.ensembleName != null)
            {
                equality = -1;
            }
            else if (this.ensembleName != null && other.ensembleName == null)
			{
				equality = 1;
			}
            else
            {
                equality = this.ensembleName.compareTo(other.getEnsembleName());
            }

            if (equality == 0 && !(this.memberIndex == null && other.memberIndex == null)) {
            	if (this.memberIndex == null && other.memberIndex != null)
				{
					equality = -1;
				}
				else if (this.memberIndex != null && other.memberIndex == null)
				{
					equality = 1;
				}
				else
				{
					equality =
							this.memberIndex.compareTo( other.getMemberIndex() );
				}
            }

            if (equality == 0 && !(this.qualifierID == null && other.qualifierID == null))
            {
            	if (this.qualifierID == null && other.qualifierID != null)
				{
					equality = -1;
				}
				else if (this.qualifierID != null && other.qualifierID == null)
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

        public short fillCount()
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
            return !(this.getEnsembleName() == null || this.getEnsembleName().isEmpty());
        }

        private boolean hasMemberIndex()
        {
            return !(this.getMemberIndex() == null || this.getMemberIndex().isEmpty());
        }

        private boolean hasQualifier()
        {
            return !(this.getQualifierID() == null || this.getQualifierID().isEmpty());
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
            if (other == null || this.fillCount() > other.fillCount())
            {
                return 0;
            }
            else if (this.equals(other))
            {
                return 3;
            }

            if ((this.hasName() && other.hasName()) && this.getEnsembleName().equalsIgnoreCase(other.getEnsembleName()))
            {
                if(!(this.hasMemberIndex() || other.hasMemberIndex()) || !this.hasMemberIndex())
                {
                    return 1;
                }
                else if ((this.hasMemberIndex() && !other.hasMemberIndex()) ||
                        !this.getMemberIndex().equalsIgnoreCase(other.getMemberIndex()))
                {
                    return 0;
                }

                if (!(this.hasQualifier() || other.hasQualifier()) || !this.hasQualifier())
                {
                    return 2;
                }
                else if ((this.hasQualifier() && !other.hasQualifier()) || !this.getQualifierID().equalsIgnoreCase(other.getQualifierID()))
                {
                    return 0;
                }

                return 3;
            }

            return 0;
        }

        @Override
        public String toString() {
            return "EnsembleKey: " + String.valueOf(this.ensembleName) + ", " +
                    String.valueOf(this.memberIndex) + ", " +
                    String.valueOf(this.qualifierID);
        }

        private final String ensembleName;
        private final String qualifierID;
        private final String memberIndex;
	}
}
