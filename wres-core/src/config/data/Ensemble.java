package config.data;

import collections.ThreeTuple;

public final class Ensemble extends ThreeTuple<String, String, String>
{
	public Ensemble(String ensemble_name, String ensemblemember_id, String qualifier) {
		super(ensemble_name, ensemblemember_id, qualifier);
	}
	
	public String get_ensemble_name()
	{
		return get_item_one();
	}
	
	public String get_ensemblemember_id()
	{
		return get_item_two();
	}
	
	public String get_qualifier()
	{
		return get_item_three();
	}
}
