package wres.io.config.specification;

import wres.io.data.caching.Ensembles;

/**
 * Represents information about an Ensemble from the configuration
 * @author Christopher Tubbs
 *
 */
public final class EnsembleSpecification {
	public EnsembleSpecification(String ensemble_name, String ensemblemember_id, String qualifier) {
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
	
	public String getID() throws Exception
	{
	    return String.valueOf(Ensembles.getEnsembleID(this.name, this.memberID, this.memberID));
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
