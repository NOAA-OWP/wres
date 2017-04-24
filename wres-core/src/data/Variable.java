/**
 * 
 */
package data;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import data.definition.VariableDef;
import config.SystemConfig;
import collections.RecentUseList;
import util.Database;

@Deprecated
/**
 * @author ctubbs
 *
 * @deprecated: Relies on the old schema
 */
public class Variable {

	private static final int MAX_DEFINITIONS = 10;
	
	private static Map<Integer, VariableDef> definitions = new ConcurrentHashMap<Integer, VariableDef>();
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
	
	private static synchronized boolean load_by_id(int id)
	{
		boolean loaded = false;
		if (!definitions.containsKey(id))
		{
			String load_script = load_by_id + String.valueOf(id) + ";";
			try {
				Connection connection = Database.getConnection();
				Statement query = connection.createStatement();
				query.setFetchSize(SystemConfig.instance().get_fetch_size());
				VariableDef new_variable = new VariableDef(query.executeQuery(load_script));
				query.close();
				Database.returnConnection(connection);
				String variable_name = new_variable.get_name();
				name_index.put(variable_name, id);
				definitions.put(id, new_variable);
				update_recently_used(id);
				loaded = true;
			} catch (SQLException e) {
				System.err.println("The variable with id: '" + String.valueOf(id) + "' could not be loaded...");
				e.printStackTrace();
			}
		}
		
		return loaded;
	}
	
	private static synchronized boolean load_by_name(String variable_name)
	{
		Integer id = null;
		boolean loaded = false;
		if (!name_index.containsKey(variable_name))
		{
			String load_script = load_by_name + variable_name + "';";
			try {
				Connection connection = Database.getConnection();
				Statement query = connection.createStatement();
				query.setFetchSize(SystemConfig.instance().get_fetch_size());
				VariableDef new_variable = new VariableDef(query.executeQuery(load_script));
				query.close();
				Database.returnConnection(connection);

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
			loaded = true;
		}		
		
		return loaded;
	}
	
	private static synchronized void update_recently_used(int id)
	{
		recently_used_ids.add(id);
		if (recently_used_ids.size() >= MAX_DEFINITIONS)
		{
			int last_id = recently_used_ids.drop_last();
			VariableDef last = definitions.get(last_id);
			definitions.remove(last_id);
			name_index.remove(last.get_name());
		}
	}
	
	public static synchronized void add(VariableDef new_definition) throws Exception
	{
		new_definition.validate();
		
		if (!load_by_name(new_definition.get_name()))
		{
			String add = String.format(add_script,
									   new_definition.get_name(),
									   new_definition.get_type(),
									   new_definition.get_description(),
									   new_definition.get_measurementunit_id());
			Database.execute(add);
			load_by_name(new_definition.get_name());
		}
	}
	
	private static final String load_by_id = "SELECT variable_name,\n"
											  + "	variable_type,\n"
											  + "	description,\n"
											  + "	measurementunit_id,\n"
											  + "	variable_id\n"
											  + "FROM Variable\n"
											  + "WHERE variable_id = ";
	
	private static final String load_by_name = "SELECT variable_id,\n"
											   + "	variable_type,\n"
											   + "	description,\n"
											   + "	measurementunit_id,\n"
											   + "	variable_name\n"
											   + "FROM Variable\n"
											   + "WHERE variable_name = '";
	
	private static final String add_script = "INSERT INTO Variable (\n"
											+ "		variable_name,\n"
											+ "		variable_type,\n"
											+ "		description,\n"
											+ "		measurementunit_id\n"
											+ ")\n"
											+ "SELECT '%s', '%s', '%s', %d\n"
											+ "WHERE NOT EXISTS (\n"
											+ "		SELECT 1\n"
											+ "		FROM Variable\n"
											+ "		WHERE variable_name == '%s'\n"
											+ ");";
}
