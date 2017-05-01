/**
 * 
 */
package data.details;

import java.sql.SQLException;

import collections.Triplet;

/**
 * Describes basic information used to define an Ensemble from the database
 * @author Christopher Tubbs
 */
public final class EnsembleDetails extends CachedDetail<EnsembleDetails, Triplet<String, String, String>>{
	
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
	/**
	 * Compares the ensemble names, then the ensemble ids
	 */
	public int compareTo(EnsembleDetails other) {
		int comparison = this.ensemble_name.compareTo(other.ensemble_name);
		
		if (comparison == 0)
		{
			comparison = this.ensembleMemberID.compareTo(other.ensembleMemberID);
		}

		return comparison;
	}

	@Override
	public Triplet<String, String, String> getKey() {
		return new Triplet<String, String, String>(this.ensemble_name, this.ensembleMemberID, this.qualifierID);
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
		
		script += "WITH new_ensemble AS" + newline;
		script += "(" + newline;
		script += "		INSERT INTO wres.Ensemble(ensemble_name, qualifier_id, ensemblemember_id)" + newline;
		script += "		SELECT '" + ensemble_name + "', " + getQualifierID() + ", " + getEnsembleMemberID() + newline;
		script += "		WHERE NOT EXISTS (" + newline;
		script += "			SELECT 1" + newline;
		script += "			FROM wres.Ensemble" + newline;
		script += "			WHERE ensemble_name = '" + ensemble_name + "'" + newline;
		script += "				AND ensemblemember_id = " + getEnsembleMemberID() + newline;
		script += "		)" + newline;
		script += "		RETURNING ensemble_id" + newline;
		script += ")" + newline;
		script += "SELECT ensemble_id" + newline;
		script += "FROM new_ensemble" + newline + newline;
		script += "";
		script += "UNION" + newline + newline;
		script += "";
		script += "SELECT ensemble_id" + newline;
		script += "FROM wres.Ensemble" + newline;
		script += "WHERE ensemble_name = '" + ensemble_name + "'" + newline;
		script += "		AND ensemblemember_id = " + getEnsembleMemberID() + ";";
		return script;
	}
}
