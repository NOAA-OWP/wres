package config.data;

import collections.ThreeTuple;

public final class Ensemble extends ThreeTuple<String, String, String>
{
	public Ensemble(String ensemble_name, String ensemblemember_id, String qualifier) {
		super(ensemble_name, ensemblemember_id, qualifier);
	}
	
	public String getEnsembleName()
	{
		return getItemOne();
	}
	
	public String getEnsemblememberID()
	{
		return getItemTwo();
	}
	
	public String getQualifier()
	{
		return getItemThree();
	}
	
	@Override
	public String toString() {
		String description = "Ensemble - Name: '";
		description += getEnsembleName();
		description += "', Member ID: ";
		description += getEnsemblememberID();
		description += ", Qualifier: '";
		description += getQualifier();
		description += "'";
		description += System.lineSeparator();
		
		return description;
	}
}
