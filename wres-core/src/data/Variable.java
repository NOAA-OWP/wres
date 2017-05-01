/**
 * 
 */
package data;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import data.definition.VariableDef;
import collections.RecentUseList;
import util.Database;

@Deprecated
/**
 * @author Christopher Tubbs
 *
 * @deprecated: Relies on the old schema
 */
public class Variable {

	private static final int MAX_DEFINITIONS = 10;
	
	private static Map<Integer, VariableDef> definitions = new ConcurrentHashMap<Integer, VariableDef>();
	private static Map<String, Integer> name_index = new ConcurrentHashMap<String, Integer>();
	private static RecentUseList<Integer> recently_used_ids = new RecentUseList<Integer>();
	
	/**
	 * Returns the ID from public.variable based on its name
	 * @param variable_name The name of the variable
	 * @return The ID of the variable
	 * @throws SQLException
	 */
	public static Integer get_variable_id(String variable_name) throws SQLException
	{
		load_by_name(variable_name);
		return name_index.get(variable_name);
	}
	
	/**
	 * Returns the type of the variable based on its variable name
	 * @param variable_name The name of the variable
	 * @return The name of the data type of the variable
	 * @throws SQLException
	 */
	public static String get_type(String variable_name) throws SQLException
	{
		int id = get_variable_id(variable_name);
		return definitions.get(id).get_type();
	}
	
	/**
	 * Returns the long name of the variable based on its variable name
	 * @param variable_name The name of the variable
	 * @return The long name of the variable
	 * @throws SQLException
	 */
	public static String get_description(String variable_name) throws SQLException
	{
		int id = get_variable_id(variable_name);
		return definitions.get(id).get_description();
	}
	
	/**
	 * Returns the default unit of measurement for the variable based on its name
	 * @param variable_name The name of the variable
	 * @return The ID of the default unit of measurement
	 * @throws SQLException
	 */
	public static int get_measurementunit_id(String variable_name) throws SQLException
	{
		int id = get_variable_id(variable_name);
		return definitions.get(id).get_measurementunit_id();
	}
	
	/**
	 * Returns the short name of a variable based on its ID
	 * @param id The ID of the variable 
	 * @return The short name of the variable
	 * @throws SQLException
	 */
	public static String get_name(int id) throws SQLException
	{
		load_by_id(id);
		return definitions.get(id).get_name();
	}
	
	/**
	 * Returns the name of the data type of a variable based on its ID
	 * @param id The ID of the variable
	 * @return The name of the data type of the variable
	 * @throws SQLException
	 */
	public static String get_type(int id) throws SQLException
	{
		load_by_id(id);
		return definitions.get(id).get_type();
	}
	
	/**
	 * Returns the long name of the variable based on its ID
	 * @param id The ID of the variable
	 * @return The long name of the variable
	 * @throws SQLException
	 */
	public static String get_description(int id) throws SQLException
	{
		load_by_id(id);
		return definitions.get(id).get_description();
	}
	
	/**
	 * Returns the ID of the unit of measurement for a variable based on its ID
	 * @param id The ID of the variable
	 * @return The ID of the unit of measurement
	 * @throws SQLException
	 */
	public static int get_measurementunit_id(int id) throws SQLException
	{
		load_by_id(id);
		return definitions.get(id).get_measurementunit_id();
	}
	
	/**
	 * Loads a variable from the database based on its ID
	 * @param id The ID of the variable to load
	 * @return True if the variable was loaded from the database
	 * @throws SQLException
	 */
	private static synchronized boolean load_by_id(int id) throws SQLException
	{
		boolean loaded = false;
		if (!definitions.containsKey(id))
		{
			String load_script = load_by_id + String.valueOf(id) + ";";
			Connection connection = null;
			try {
				connection = Database.getConnection();

				VariableDef new_variable = new VariableDef(Database.getResults(connection, load_script));

				String variable_name = new_variable.get_name();
				name_index.put(variable_name, id);
				definitions.put(id, new_variable);
				update_recently_used(id);
				loaded = true;
			} catch (SQLException e) {
				System.err.println("The variable with id: '" + String.valueOf(id) + "' could not be loaded...");
				throw e;
			} finally {
			    if (connection != null) {
			        Database.returnConnection(connection);
			    }
			}
		}
		
		return loaded;
	}
	
	/**
	 * Loads a variable by name into the cache
	 * @param variable_name The name of the variable to load
	 * @return True if the variable was successfully loaded
	 * @throws SQLException An error is thrown if the database connection could not be adequately returned
	 */
	private static synchronized boolean load_by_name(String variable_name) throws SQLException
	{
		Integer id = null;
		boolean loaded = false;
		if (!name_index.containsKey(variable_name))
		{
			String load_script = load_by_name + variable_name + "';";
			Connection connection = null;
			try {
				connection = Database.getConnection();

				VariableDef new_variable = new VariableDef(Database.getResults(connection, load_script));
				id = new_variable.get_variable_id();
				name_index.put(variable_name, id);
				definitions.put(id, new_variable);
				
			} catch (SQLException e) {
				System.err.println("The variable '" + variable_name + "' could not be loaded...");
				e.printStackTrace();
			} finally {
                Database.returnConnection(connection);
			}
		}
		
		if (id != null)
		{
			update_recently_used(id);
			loaded = true;
		}		
		
		return loaded;
	}
	
	/**
	 * Updates the list of recently used variables based on the ID of a variable
	 * @param id The id of a recently used variable
	 */
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
	
	/**
	 * Adds the definition of a variable to the cache
	 * @param new_definition The definition of a variable
	 * @throws Exception
	 */
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
	
	/**
	 * Script template used to load information about a variable based on its ID
	 */
	private static final String load_by_id = "SELECT variable_name,\n"
											  + "	variable_type,\n"
											  + "	description,\n"
											  + "	measurementunit_id,\n"
											  + "	variable_id\n"
											  + "FROM Variable\n"
											  + "WHERE variable_id = ";
	
	/**
	 * Script template used to load information about a variable based on its name
	 */
	private static final String load_by_name = "SELECT variable_id,\n"
											   + "	variable_type,\n"
											   + "	description,\n"
											   + "	measurementunit_id,\n"
											   + "	variable_name\n"
											   + "FROM Variable\n"
											   + "WHERE variable_name = '";
	
	/**
	 * Script template used to add a variable into the database
	 */
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
