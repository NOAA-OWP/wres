package config.data;

/**
 * Represents information about an Ensemble from the configuration
 * @author Christopher Tubbs
 *
 */
public final class Ensemble {
	public Ensemble(String ensemble_name, String ensemblemember_id, String qualifier) {
		this.name = ensemble_name;
		this.memberID = ensemblemember_id;
		this.qualifier = qualifier;
	}
	
	public String getName()
	{
		return this.name;
	}
	
	public String getMemberID()
	{
		return this.memberID;
	}
	
	public String getQualifier()
	{
		return this.qualifier;
	}
	
	@Override
	public String toString() {
		String description = "Ensemble - Name: '";
		description += getName();
		description += "', Member ID: ";
		description += getMemberID();
		description += ", Qualifier: '";
		description += getQualifier();
		description += "'";
		description += System.lineSeparator();
		
		return description;
	}
	
	private final String name;
	private final String memberID;
	private final String qualifier;
}
