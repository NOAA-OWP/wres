package wres.io.data.details;

import java.sql.SQLException;

import wres.io.data.details.EnsembleDetails.EnsembleKey;

/**
 * Describes basic information used to define an Ensemble from the database
 * @author Christopher Tubbs
 */
public final class EnsembleDetails extends CachedDetail<EnsembleDetails, EnsembleKey>{
	
	// The name of the ensemble being represented
	private String ensemble_name = null;
	
	// The "numeric" id of the member of the ensemble
	private String ensembleMemberID = null;
	
	// The serial id of the ensemble in the database
	private Integer ensembleID = null;

	// The qualifier for the ensemble
	public String qualifierID = null;
	
	/**
	 * Updates the ensemble name if necessary. If the update occurs, the serial id is reset
	 * @param ensemble_name The new name for the ensemble
	 */
	public void setEnsembleName(String ensemble_name)
	{
		if (this.ensemble_name == null || !this.ensemble_name.equalsIgnoreCase(ensemble_name))
		{
			this.ensemble_name = ensemble_name;
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
	 * Retrieves the id of this particular ensemble. If it doesn't exist, it is generated.
	 * @return The serial id of the ensemble
	 * @throws SQLException Throws SQLException if the ID generation fails
	 */
	public int get_ensemble_id() throws SQLException
	{
		if (ensembleID == null)
		{
			save();
		}
		
		return ensembleID;
	}
	
	/**
	 * Returns the proper string representation of the qualifier ID of the ensemble
	 * @return The qualifier id wrapped in single quotes or the unwrapped String stating null
	 */
	private String getQualifierID()
	{
		String id = null;
		
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
	public int compareTo(EnsembleDetails other) {
		int comparison = this.ensemble_name.compareTo(other.ensemble_name);
		
		if (comparison == 0)
		{
			comparison = this.ensembleMemberID.compareTo(other.ensembleMemberID);
		}

		return comparison;
	}

	@Override
	public EnsembleKey getKey() {
		return new EnsembleKey(this.ensemble_name, this.qualifierID, this.ensembleMemberID);
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
		script += "		SELECT '" + ensemble_name + "', " + getQualifierID() + ", " + getEnsembleMemberID() + NEWLINE;
		script += "		WHERE NOT EXISTS (" + NEWLINE;
		script += "			SELECT 1" + NEWLINE;
		script += "			FROM wres.Ensemble" + NEWLINE;
		script += "			WHERE ensemble_name = '" + ensemble_name + "'" + NEWLINE;
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
		script += "WHERE ensemble_name = '" + ensemble_name + "'" + NEWLINE;
		script += "		AND ensemblemember_id = " + getEnsembleMemberID() + ";";
		return script;
	}
	
	public static EnsembleKey createKey(String ensembleName, String qualifierID, String memberIndex)
	{
	    return new EnsembleDetails().new EnsembleKey(ensembleName, qualifierID, memberIndex);
	}
	
	public class EnsembleKey implements Comparable<EnsembleKey>
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
            int equality = this.ensembleName.compareTo(other.getEnsembleName());
            if (equality == 0) {
                equality = this.memberIndex.compareTo(other.getMemberIndex());
            }
            if (equality == 0)
            {
                equality = this.qualifierID.compareTo(other.getQualifierID());
            }
            return equality;
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
        
        public Integer getSimilarity(EnsembleKey other)
        {
            if (this.equals(other))
            {
                return 3;
            }

			Integer similarity = 0;
            
            if (!(this.getEnsembleName() == null || this.getEnsembleName().isEmpty()) &&
                !(other.getEnsembleName() == null || other.getEnsembleName().isEmpty()) &&
                this.getEnsembleName().equalsIgnoreCase(other.getEnsembleName()))
            {
                similarity++;
            }
            
            if (similarity == 1 && !(this.getMemberIndex() == null || other.getMemberIndex() == null))
            {
                if (this.getMemberIndex().equalsIgnoreCase(other.getMemberIndex()))
                {
                    similarity++;
                }
            }
            
            if (similarity == 2 &&
                !(this.getQualifierID() == null || other.getQualifierID() == null) &&
                this.getQualifierID().equalsIgnoreCase(other.getQualifierID()))
            {
                similarity++;
            }
            
            return similarity;
        }
	    
        private String ensembleName;
        private String qualifierID;
        private String memberIndex;
	}
}
