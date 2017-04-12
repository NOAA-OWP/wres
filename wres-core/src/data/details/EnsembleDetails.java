/**
 * 
 */
package data.details;

import java.sql.SQLException;

import util.Database;

/**
 * Describes basic information used to define 
 */
public final class EnsembleDetails implements Comparable<EnsembleDetails>
{
	// System specific representation of a newline
	private final static String newline = System.lineSeparator();
	
	// The name of the ensemble being represented
	private String ensemble_name = null;
	
	// The "numeric" id of the member of the ensemble
	private String ensemblemember_id = null;
	
	// The serial id of the ensemble in the database
	private Integer ensemble_id = null;

	// The qualifier for the ensemble
	public String qualifier_id = null;
	
	/**
	 * Updates the ensemble name if necessary. If the update occurs, the serial id is reset
	 * @param ensemble_name The new name for the ensemble
	 */
	public void set_ensemble_name(String ensemble_name)
	{
		if (this.ensemble_name == null || !this.ensemble_name.equalsIgnoreCase(ensemble_name))
		{
			this.ensemble_name = ensemble_name;
			this.ensemble_id = null;
		}
	}
	
	/**
	 * Updates the ensemble member id if necessary. If the update occurs, the serial id is reset
	 * @param ensemblemember_id The new id for the ensemble member
	 */
	public void set_ensemblemember_id(String ensemblemember_id)
	{
		if (this.ensemblemember_id == null || !this.ensemblemember_id.equalsIgnoreCase(ensemblemember_id))
		{
			this.ensemblemember_id = ensemblemember_id;
			this.ensemble_id = null;
		}
	}
	
	/**
	 * Retrieves the id of this particular ensemble. If it doesn't exist, it is generated.
	 * @return The serial id of the ensemble
	 * @throws SQLException Throws SQLException if the ID generation fails
	 */
	public int get_ensemble_id() throws SQLException
	{
		if (ensemble_id == null)
		{
			save();
		}
		
		return ensemble_id;
	}
	
	/**
	 * Returns the proper string representation of the qualifier ID of the ensemble
	 * @return The qualifier id wrapped in single quotes or the unwrapped String stating null
	 */
	private String get_qualifier_id()
	{
		String id = null;
		
		if (qualifier_id == null)
		{
			id = "null";
		}
		else
		{
			id = "'" + qualifier_id + "'";
		}
		
		return id;
	}
	
	/**
	 * Returns a sanitized version of the ensemblemember id for saving and loading values
	 * @return The id of the ensemble member if one exists, "0" otherwise
	 */
	private String get_ensemblemember_id()
	{
		String id = "0";
		
		if (ensemblemember_id != null)
		{
			id = this.ensemblemember_id;
		}
		
		return id;
	}
	
	/**
	 * Generates a SQL statement that will insert the ensemble and/or return the id of the ensemble
	 * @throws SQLException Throws a SQLException if there was an error thrown in the Database
	 */
	private void save() throws SQLException
	{
		String script = "";
		
		script += "WITH new_ensemble AS" + newline;
		script += "(" + newline;
		script += "		INSERT INTO wres.Ensemble(ensemble_name, qualifier_id, ensemblemember_id)" + newline;
		script += "		SELECT '" + ensemble_name + "', " + get_qualifier_id() + ", " + get_ensemblemember_id() + newline;
		script += "		WHERE NOT EXISTS (" + newline;
		script += "			SELECT 1" + newline;
		script += "			FROM wres.Ensemble" + newline;
		script += "			WHERE ensemble_name = '" + ensemble_name + "'" + newline;
		script += "				AND ensemblemember_id = " + get_ensemblemember_id() + newline;
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
		script += "		AND ensemblemember_id = " + get_ensemblemember_id() + ";";
		
		ensemble_id = Database.get_result(script, "ensemble_id");
	}

	@Override
	/**
	 * Compares the ensemble names, then the ensemble ids
	 */
	public int compareTo(EnsembleDetails other) {
		int comparison = this.ensemble_name.compareTo(other.ensemble_name);
		
		if (comparison == 0)
		{
			comparison = this.ensemblemember_id.compareTo(other.ensemblemember_id);
		}

		return comparison;
	}
}
