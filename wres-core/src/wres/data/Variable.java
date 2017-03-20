/**
 * 
 */
package wres.data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import wres.collections.RecentUseList;
import wres.util.Database;

/**
 * @author ctubbs
 *
 */
public class Variable {

	private static final int MAX_DEFINITIONS = 10;
	
	private static Map<Integer, Definition> definitions = new ConcurrentHashMap<Integer, Definition>();
	private static Map<String, Integer> name_index = new ConcurrentHashMap<String, Integer>();
	private static RecentUseList<Integer> recently_used_ids = new RecentUseList<Integer>();
	
	public static Integer get_variable_id(String variable_name)
	{
		load_by_name(variable_name);
		return name_index.get(variable_name);
	}
	
	public static String get_type(String variable_name)
	{
		int id = get_variable_id(variable_name);
		return definitions.get(id).get_type();
	}
	
	public static String get_description(String variable_name)
	{
		int id = get_variable_id(variable_name);
		return definitions.get(id).get_description();
	}
	
	public static int get_measurementunit_id(String variable_name)
	{
		int id = get_variable_id(variable_name);
		return definitions.get(id).get_measurementunit_id();
	}
	
	public static String get_name(int id)
	{
		load_by_id(id);
		return definitions.get(id).get_name();
	}
	
	public static String get_type(int id)
	{
		load_by_id(id);
		return definitions.get(id).get_type();
	}
	
	public static String get_description(int id)
	{
		load_by_id(id);
		return definitions.get(id).get_description();
	}
	
	public static int get_measurementunit_id(int id)
	{
		load_by_id(id);
		return definitions.get(id).get_measurementunit_id();
	}
	
	private static synchronized void load_by_id(int id)
	{
		if (!definitions.containsKey(id))
		{
			String load_script = load_by_id + String.valueOf(id) + ";";
			try {
				Definition new_variable = new Definition(Database.execute_for_result(load_script));
				String variable_name = new_variable.get_name();
				name_index.put(variable_name, id);
				definitions.put(id, new_variable);
				
			} catch (SQLException e) {
				System.err.println("The variable with id: '" + String.valueOf(id) + "' could not be loaded...");
				e.printStackTrace();
			}
		}
		
		update_recently_used(id);
		
	}
	
	private static synchronized void load_by_name(String variable_name)
	{
		Integer id = null;
		if (!name_index.containsKey(variable_name))
		{
			String load_script = load_by_name + variable_name + "';";
			try {
				Definition new_variable = new Definition(Database.execute_for_result(load_script));
				id = new_variable.get_variable_id();
				name_index.put(variable_name, id);
				definitions.put(id, new_variable);
				
			} catch (SQLException e) {
				System.err.println("The variable '" + variable_name + "' could not be loaded...");
				e.printStackTrace();
			}
		}
		
		if (id != null)
		{
			update_recently_used(id);
		}		
	}
	
	private static synchronized void update_recently_used(int id)
	{
		recently_used_ids.add(id);
		if (recently_used_ids.size() >= MAX_DEFINITIONS)
		{
			int last_id = recently_used_ids.drop_last();
			Definition last = definitions.get(last_id);
			definitions.remove(last_id);
			name_index.remove(last.get_name());
		}
	}
	
	private static class Definition
	{
		public Definition(ResultSet result) throws SQLException
		{
			if (result.isBeforeFirst())
			{
				result.next();
			}
			
			this.variable_id = result.getInt("variable_id");
			this.variable_name = result.getString("variable_name");
			this.variable_type = result.getString("variable_type");
			this.description = result.getString("description");
			this.measurementunit_id = result.getInt("measurementunit_id");
		}
		
		public int get_variable_id()
		{
			return variable_id;
		}
		
		public String get_name()
		{
			return variable_name;
		}
		
		public String get_type()
		{
			return variable_type;
		}
		
		public String get_description()
		{
			return description;
		}
		
		public int get_measurementunit_id()
		{
			return measurementunit_id;
		}
		
		private int variable_id;
		private String variable_name;
		private String variable_type;
		private String description;
		private int measurementunit_id;
	}
	
	private static final String load_by_id = "SELECT variable_name,\n"
											  + "	variable_type,\n"
											  + "	description,\n"
											  + "	measurementunit_id\n"
											  + "FROM Variable\n"
											  + "WHERE variable_id = ";
	
	private static final String load_by_name = "SELECT variable_id,\n"
											   + "	variable_type,\n"
											   + "	description,\n"
											   + "	measurementunit_id,\n"
											   + "FROM Variable\n"
											   + "WHERE variable_name = '";
}
