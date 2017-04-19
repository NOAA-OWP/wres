/**
 * 
 */
package reading.ucar;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import collections.AssociatedPair;
import collections.Triplet;
import concurrency.Executor;
import config.SystemConfig;
import reading.BasicSource;
import util.Database;
import util.Utilities;

/**
 * @author ctubbs
 *
 */
public class NetCDFSource extends BasicSource {

	private class ValueInserter implements Runnable
	{
		public ValueInserter(List<Triplet<Integer, Integer, Double>> parameters)
		{
			this.parameters = parameters;
			script_builder = new StringBuilder(insert_header);
		}

		@Override
		public void run() {
			boolean add_comma = false;
			
			for (Triplet<Integer, Integer, Double> parameter : parameters)
			{
				if (add_comma)
				{
					script_builder.append(",\n");
				}
				else
				{
					add_comma = true;
				}
				
				/*script_builder.append(String.format(value_template, 
													parameter.item_one, 
													parameter.item_two, 
													parameter.item_three));*/
				script_builder.append("(");
				script_builder.append(parameter.item_one);
				script_builder.append(", ");
				script_builder.append(parameter.item_two);
				script_builder.append(", ");
				script_builder.append(parameter.item_three);
				script_builder.append(")");
			}
			
			script_builder.append(";");
			
			try {
				/*Executor.execute(new Runnable() {
					@Override
					public void run() {
						try {*/
							Database.execute(script_builder.toString());
							//System.out.println("Values added!");
						/*} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}						
					}					
				});*/
				//Executor.submit(new SQLExecutor(script_builder.toString()));
				//Database.execute(script_builder.toString());
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		
		private List<Triplet<Integer, Integer, Double>> parameters;
		private final String insert_header = "INSERT INTO wres.ForecastValue\n"
										   + "(\n"
										   + "		forecastensemble_id,\n"
										   + "		lead,\n"
										   + "		forecasted_value\n"
										   + ")\n"
										   + "VALUES\n";
		private final String value_template = "(%d, %d, %f)";
		private StringBuilder script_builder;
	}
	
	private class VariableInserter implements Runnable
	{
		public VariableInserter(Variable variable_to_insert)
		{
			this.variable_to_insert = variable_to_insert;
		}
		@Override
		public void run() {
			try {
				boolean exists = Database.get_result(String.format(variable_exists_script, variable_to_insert.getShortName()), "variable_exists");
				if (!exists)
				{
					System.out.println("Adding the " + variable_to_insert.getShortName() + " variable to the database...");
					Database.execute(String.format(save_measurement_script, variable_to_insert.getUnitsString(), variable_to_insert.getUnitsString()));
					String script = String.format(save_variable_script,
							variable_to_insert.getShortName(),
							variable_to_insert.getDescription(),
							variable_to_insert.getShortName(),
							variable_to_insert.getDataType().toString(),
							variable_to_insert.getDescription(),
							variable_to_insert.getUnitsString(),
							variable_to_insert.getShortName(),
							variable_to_insert.getDescription());
					
					Database.execute(script);
					
					script = String.format(get_variable_id_script, variable_to_insert.getShortName(), variable_to_insert.getDescription());
					
					Integer variable_id = Database.get_result(script, "variable_id");

					
					System.out.println(variable_to_insert.getShortName() + " successfully added to the database.");
					
					if (variable_to_insert.getShortName().equals("station_id") || variable_to_insert.getShortName().equals("feature_id"))
					{
						System.out.println("Adding feature information...");
						add_features();
					}
					else
					{
						int x_length = get_x_length(variable_to_insert);
						int y_length = get_y_length(variable_to_insert);
						
						System.out.println("Adding positions for all " + variable_to_insert.getShortName() + " values.");
						Database.execute(String.format(save_variableposition_script,
													   variable_id,
													   x_length,
													   y_length,
													   variable_id));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}			
		}
		
		private void add_features() throws Exception
		{
			Database.execute("DELETE FROM wres.FeaturePosition;");
			Array features = variable_to_insert.read();
			final int insert_limit = 8000;
			int insert_count = 0;
			final String insert = "INSERT INTO wres.featureposition VALUES ";
			StringBuilder script = new StringBuilder(insert);
			boolean add_comma = false;
			
			for (int feature_index = 0; feature_index < features.getSize(); ++feature_index)
			{
				if (add_comma)
				{
					script.append(", ");
				}
				else
				{
					add_comma = true;
				}
				
				script.append("(");
				script.append(feature_index);
				script.append(", ");
				script.append(features.getInt(feature_index));
				script.append(")");
				insert_count++;
				
				if (insert_count >= insert_limit)
				{
					script.append(";");
					
					fire_query(script.toString());
					
					insert_count = 0;
					script = new StringBuilder(insert);
					add_comma = false;
				}
			}
			
			if (insert_count > 0)
			{
				script.append(";");
				fire_query(script.toString());
			}
		}
		
		private void fire_query(String query) throws SQLException
		{
			Database.execute(query);
			/*Executor.execute(new Runnable() {
				
				@Override
				public void run() {
					try {
						Database.execute(query);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				
				public Runnable set_query(String query)
				{
					this.query = query;
					return this;
				}
				
				private String query = "";
			}.set_query(query));*/
		}
		
		private Variable variable_to_insert = null;
	}
	/**
	 * 
	 */
	public NetCDFSource(String filename) {
		this.set_filename(filename);
	}
	
	@Override
	public void save_observation() throws Exception
	{
		
	}
	
	@Override
	public void save_forecast() throws Exception
	{
		NetcdfFile source = get_source();
		Attribute attr = source.findGlobalAttributeIgnoreCase("model_initialization_time");
		this.model_initialization_time = attr.getStringValue().replaceAll("_", " ");
		
		attr = source.findGlobalAttributeIgnoreCase("missing_value");
		if (attr != null)
		{
			this.missing_value = attr.getNumericValue().doubleValue();
		}

		save_variables();
		//save_values();
	}
	
	public void save() throws Exception
	{
		if (this.is_observation())
		{
			this.save_observation();
		}
		else {
			this.save_forecast();
		}
	}
	
	private void save_values() throws Exception
	{
		NetcdfFile source = get_source();
		int insert_limit = 10000;
		List<Triplet<Integer, Integer, Double>> values_to_insert;
		int dimension_count;
		Double found_value = null;
		Index index = null;

		for (Variable var : source.getVariables())
		{
			dimension_count = var.getDimensions().size();
			if (dimension_count > 1 || var.getDimension(0).getLength() > 1)
			{
				System.out.println("Saving values for " + var.getDescription());
				
				Double fill_value = missing_value;
				Attribute fill = var.findAttribute("_FillValue");
				
				if (fill != null)
				{
					fill_value = fill.getNumericValue().doubleValue();
				}
				
				values_to_insert = new ArrayList<Triplet<Integer, Integer, Double>>();
				load_variable_positions(var.getShortName());
				
				int x_length = get_x_length(var);
				
				int x_position = 0;
				int y_position = -1;
				
				switch (dimension_count)
				{
				case 2:
					y_position = 1;
				case 3:
					x_position = 1;
					y_position = 2;
				}
				
				int[] origin = new int[dimension_count];
				int[] shape = new int[dimension_count];
				int[] value_accessor = new int[dimension_count];
				
				Arrays.fill(value_accessor, 0);
				Arrays.fill(origin, 0);
				Arrays.fill(shape, 1);

				if (y_position > 0)
				{
					shape[y_position] = get_y_length(var);
				}
				
				if (dimension_count == 1)
				{
					int current_index = 0;
					while (current_index < x_length)
					{
						origin[0] = current_index;
						shape[0] = Math.min(insert_limit, x_length - insert_limit);
						Array block = var.read(origin, shape);
												
						while (block.hasNext())
						{
							found_value = block.nextDouble();
							
							if (found_value != fill_value)
							{
								values_to_insert.add(new Triplet<Integer, Integer, Double>(get_position(current_index, 0), get_lead(), found_value));
							}
							current_index++;
						}

						Executor.execute(new ValueInserter(values_to_insert));
						values_to_insert = new ArrayList<Triplet<Integer, Integer, Double>>();
					}
				}
				
				/*for (int x_index = 0; x_index < x_length; ++x_index)
				{
					origin[x_position] = x_index;
					Array block = var.read(origin, shape);

					index = block.getIndex();
					block.reduce();
					
					if (dimension_count > 1)
					{
						for (int y_index = 0; y_index < shape[y_position]; ++y_index)
						{
							//value_accessor[y_position] = y_index;
							//index.set(value_accessor);
							index.set(x_index, y_index);
							found_value = block.getDouble(index);
							
							if (found_value != fill_value)
							{
								values_to_insert.add(new Triplet<Integer, Integer, Double>(get_position(x_index, y_index), get_lead(), found_value));
							}
							
							if (values_to_insert.size() >= insert_limit)
							{
								System.out.println("Dispatching job to save values...");
								//Executor.execute(new ValueInserter(values_to_insert));
								values_to_insert = new ArrayList<Triplet<Integer, Integer, Double>>();
							}
						}
					}
					else
					{
						//index.set(x_index);
						found_value = block.getDouble(0);
						//found_value = block.getDouble(index);
						
						if (found_value != fill_value)
						{
							values_to_insert.add(new Triplet<Integer, Integer, Double>(get_position(x_index, 0), get_lead(), found_value));
						}
						
						if (values_to_insert.size() >= insert_limit)
						{
							//Executor.execute(new ValueInserter(values_to_insert));
							System.out.println("Dispatching job to save values...");
							values_to_insert = new ArrayList<Triplet<Integer, Integer, Double>>();
						}
					}
				}*/
				
				if (values_to_insert.size() > 0)
				{
					//Executor.execute(new ValueInserter(values_to_insert));
					System.out.println("Dispatching one last job to save values...");
					//values_to_insert = new ArrayList<Triplet<Integer, Integer, Double>>();
				}
			}
		}
		Executor.complete();
	}
	
	private int get_position(int x_index, int y_index)
	{
		return this.variable_positions.get(x_index, y_index);
	}
	
	private void load_variable_positions(String variable_name) throws SQLException
	{
		System.out.println("Loading a new set of value positions for " + variable_name);
		this.variable_positions = new AssociatedPair<Integer, Integer, Integer>();
		Connection connection = Database.get_connection();
		connection.setAutoCommit(false);
		Statement query = connection.createStatement();
		query.setFetchSize(SystemConfig.instance().get_fetch_size());
		
		ResultSet position_set = query.executeQuery(String.format(load_variablepositions_script, 
																		   get_forecast_id(), 
																		   variable_name, 
																		   this.ensemble_name));
		while (position_set.next())
		{
			this.variable_positions.put(position_set.getInt("x_position"), 
										position_set.getInt("y_position"), 
										position_set.getInt("forecastensemble_id"));
		}
		
		Database.return_connection(connection);
		System.out.println("Positions loaded.");
	}
	
	private void save_variables() throws IOException, SQLException
	{
		NetcdfFile source = get_source();

		for (Variable var : source.getVariables())
		{
			if (var.getDimensions().size() > 1 || var.getDimension(0).getLength() > 1)
			{
				/*boolean exists = Database.get_result(String.format(variable_exists_script, var.getShortName()), "variable_exists");
				
				if (!exists)
				{
					System.out.println("Adding the " + var.getShortName() + " variable to the database...");
					Database.execute(String.format(save_measurement_script, var.getUnitsString(), var.getUnitsString()));
					String script = String.format(save_variable_script,
					  					   		  var.getShortName(),
					  					   		  var.getDescription(),
					  					   		  var.getShortName(),
					  					   		  var.getDataType().toString(),
					  					   		  var.getDescription(),
					  					   		  var.getUnitsString(),
					  					   		  var.getShortName(),
					  					   		  var.getDescription());
					
					Database.execute(script);
					
					script = String.format(get_variable_id_script, var.getShortName(), var.getDescription());
					
					Integer variable_id = Database.get_result(script, "variable_id");
					
					int x_length = get_x_length(var);
					int y_length = get_y_length(var);
					
					System.out.println("Adding positions for all " + var.getShortName() + " values.");
					Database.execute(String.format(save_variableposition_script,
												   variable_id,
												   x_length,
												   y_length,
												   variable_id));
				}*/
				
				Executor.execute(new VariableInserter(var));

				/*exists = Database.get_result(String.format(forecastensemble_exists_script, 
														   get_forecast_id(), 
														   this.ensemble_name, 
														   var.getShortName()), 
											 "forecastensemble_exists");
				
				if (!exists)
				{
					System.out.println("Adding the " + var.getShortName() + " variable to this forecast...");
					Database.execute(String.format(populate_forecastensemble_script, 
												   this.get_forecast_id(), 
												   var.getShortName(), 
												   "default", 
												   var.getUnitsString()));
				}*/
			}
		}
	}
	
	private int get_x_length(Variable var)
	{
		int length = 0;
		
		switch (var.getDimensions().size())
		{
			case 1:
				length = var.getDimension(0).getLength();
				break;
			case 2:
				length = var.getDimension(0).getLength();
				break;
			case 3:
				length = var.getDimension(1).getLength();
				break;				
		}
		
		return length;
	}
	
	private int get_y_length(Variable var)
	{
		int length = 0;
		
		switch (var.getDimensions().size())
		{
			case 2:
				length = var.getDimension(1).getLength();
				break;
			case 3:
				length = var.getDimension(2).getLength();
				break;
		}
		
		return length;
	}
	
	private NetcdfFile get_source() throws IOException
	{
		if (source == null)
		{
			source = NetcdfFile.open(get_absolute_filename());
		}
		return source;
	}
	
	private NetcdfFile source = null;
	
	private boolean is_observation()
	{
		return this.lead == 0;
	}
	
	private Integer get_forecast_hour()
	{
		if (this.forecast_hour == null)
		{
			String description = Utilities.where(get_file_parts(), (String possibility) -> {
				return possibility.endsWith("z");
			});
			
			description.replaceAll("\\D", "");
			this.forecast_hour = Integer.parseInt(description);
		}
		return this.forecast_hour;
	}
	
	private Integer get_lead()
	{
		if (this.lead == null)
		{
			String description = Utilities.where(get_file_parts(), (String possibility) -> {
				return possibility.startsWith("f") || possibility.startsWith("tm");
			});
			
			description = description.replaceAll("\\D", "");
			this.lead = Integer.parseInt(description);
		}
		return this.lead;
	}
	
	private String get_range()
	{
		if (this.range == null)
		{
			this.range = Utilities.where(get_file_parts(), (String possibility) -> {
				return possibility.endsWith("range") || possibility.endsWith("assim");
			});
		}
		return this.range;
	}
	
	private String get_data_category()
	{
		if (this.data_category == null)
		{
			String category = Utilities.where(get_file_parts(), (String possibility) -> {
				return possibility.contains("land") || possibility.contains("reservoir");
			});
			
			this.data_category = category.split("_")[0];
		}
		return this.data_category;
	}
	
	private String[] get_file_parts()
	{
		if (file_parts == null)
		{
			Path source_path = Paths.get(get_absolute_filename());
			String filename = source_path.getFileName().toString();
			filename = filename.replaceAll("\\.gz", "");
			filename = filename.replaceAll("nwm\\.", "");
			filename = filename.replaceAll("\\.nc", "");
			
			file_parts = filename.split("\\.");
		}
		
		return this.file_parts;
	}
	
	private Integer get_forecast_id() throws SQLException
	{
		if (this.forecast_id == null)
		{
			String type = get_range().replace("_range", "");
			String script = String.format(this.save_forecast_script, 
										  this.model_initialization_time, 
										  type,
										  this.model_initialization_time);
			
			Database.execute(script);
			
			script = "SELECT forecast_id FROM wres.forecast WHERE forecast_date = '" + this.model_initialization_time + "';";
			this.forecast_id = Database.get_result(String.format(get_forecast_id_script, 
																 this.model_initialization_time, 
																 type), 
													"forecast_id");
		}
		return this.forecast_id;
	}
	
	private String[] file_parts = null;
	private Integer forecast_hour = null;
	private String range = null;
	private String data_category = null;
	private Integer lead = null;
	private AssociatedPair<Integer, Integer, Integer> variable_positions = null;
	private String ensemble_name = "default";
	
	private String model_initialization_time;
	private Double missing_value = null;
	private String model_valid_output_time;
	private Integer forecast_id = null;
	
	private final String save_source_script = " DO $$\n"
											+ " BEGIN\n"
											+ "		IF EXISTS (\n"
											+ "			SELECT 1\n"
											+ "			FROM wres.Source\n"
											+ "			WHERE path = '%s'\n"
											+ "		) THEN\n"
											+ "			UPDATE wres.Source\n"
											+ "				SET output_time = '%s'\n"
											+ "			WHERE path = '%s';\n"
											+ "		ELSE\n"
											+ "			INSERT INTO wres.Source\n"
											+ "			(\n"
											+ "				path,\n"
											+ "				output_time\n"
											+ "			)\n"
											+ "			VALUES (\n"
											+ "				'%s',\n"
											+ "				'%s'\n"
											+ "			);\n"	
											+ "		END IF;\n"
											+ "END $$;";
	
	private final String save_forecast_script = "INSERT INTO wres.Forecast (forecast_date, forecasttype_id)\n"
											  + "SELECT '%s',\n"
											  + "	T.forecasttype_id\n"
											  + "FROM wres.ForecastType T\n"
											  + "WHERE T.type_name = '%s'\n"
											  + "	AND NOT EXISTS (\n"
											  + "		SELECT 1\n"
											  + "		FROM wres.Forecast\n"
											  + "		WHERE forecast_date = '%s'\n"
											  + "			AND forecasttype_id = T.forecasttype_id\n"
											  + ");";
	
	private final String get_forecast_id_script = "SELECT forecast_id\n"
												+ "FROM wres.Forecast F\n"
												+ "INNER JOIN wres.ForecastType T\n"
												+ "	ON T.forecasttype_id = F.forecasttype_id\n"
												+ "WHERE F.forecast_date = '%s'\n"
												+ "	AND T.type_name = '%s';";
	
	private final String save_variable_script = "DO $$\n"
											  + "BEGIN\n"
											  + "	IF NOT EXISTS (\n"
											  + "		SELECT 1\n"
											  + "		FROM wres.VARIABLE"
											  + "		WHERE variable_name = '%s'\n"
											  + "			AND description = '%s'\n"
											  + "	) THEN\n"
											  + "		INSERT INTO wres.Variable\n"
											  + "		(\n"
											  + "			variable_name,\n"
											  + "			variable_type,\n"
											  + "			description,\n"
											  + "			measurementunit_id\n"
											  + "		)\n"
											  + "		SELECT '%s',\n"
											  + "				'%s',\n"
											  + "				'%s',\n"
											  + "				measurementunit_id\n"
											  + "		FROM wres.measurementunit\n"
											  + "		WHERE unit_name = '%s';\n"
											  + "	END IF;\n"
											  + "END $$;";
	
	private final String forecastensemble_exists_script = "SELECT EXISTS (\n"
														+ "		SELECT 1\n"
														+ "		FROM wres.ForecastEnsemble FE\n"
														+ "		INNER JOIN wres.VariablePosition VP\n"
														+ "			ON VP.variableposition_id = FE.variableposition_id\n"
														+ "		INNER JOIN wres.Variable V\n"
														+ "			ON V.variable_id = VP.variable_id\n"
														+ "		INNER JOIN wres.Ensemble E\n"
														+ "			ON E.ensemble_id = FE.ensemble_id\n"
														+ "		WHERE FE.forecast_id = %d\n"
														+ "			AND E.ensemble_name = '%s'\n"
														+ "			AND V.variable_name = '%s'\n"
														+ ") AS forecastensemble_exists;";
	
	private final String get_variable_id_script = "SELECT variable_id\n"
												+ "FROM wres.Variable\n"
												+ "WHERE variable_name = '%s'\n"
												+ "		AND description = '%s';";
	
	private final String save_variableposition_script = "INSERT INTO wres.variableposition (variable_id, x_position, y_position)\n"
													  + "SELECT %d, X, Y\n"
													  + "FROM generate_series(0, %d) AS X\n"
													  + "CROSS JOIN generate_series(0, %d) AS Y\n"
													  + "WHERE NOT EXISTS (\n"
													  + "	SELECT 1\n"
													  + "	FROM wres.variableposition"
													  + "	WHERE variable_id = %d\n"
													  + "		AND x_position = X\n"
													  + "		AND y_position = Y\n"
													  + ");";
	
	private final String populate_forecastensemble_script = "INSERT INTO wres.forecastensemble\n"
														  + "(\n"
														  + "	forecast_id,\n"
														  + "	variableposition_id,\n"
														  + "	ensemble_id,\n"
														  + "	measurementunit_id\n"
														  + ")\n"
														  + "SELECT %d,\n"
														  + "	VP.variableposition_id,\n"
														  + "	E.ensemble_id,\n"
														  + "	M.measurementunit_id\n"
														  + "FROM wres.variableposition VP\n"
														  + "INNER JOIN wres.variable V\n"
														  + "	ON V.variable_id = VP.variable_id\n"
														  + "CROSS JOIN wres.Ensemble E\n"
														  + "CROSS JOIN wres.measurementunit M\n"
														  + "WHERE V.variable_name = '%s'\n"
														  + "	AND E.ensemble_name = '%s'\n"
														  + "	AND M.unit_name = '%s'\n"
														  + "	AND NOT EXISTS (\n"
														  + "		SELECT 1\n"
														  + "		FROM wres.forecastensemble FE\n"
														  + "		WHERE FE.variableposition_id = VP.variableposition_id\n"
														  + "			AND FE.ensemble_id = E.ensemble_id\n"
														  + "			AND FE.measurementunit_id = M.measurementunit_id\n"
														  + "	);";
	
	private final String load_variablepositions_script = "SELECT x_position, y_position, forecastensemble_id\n"
													   + "FROM wres.ForecastEnsemble FE\n"
													   + "INNER JOIN wres.VariablePosition VP\n"
													   + "	ON VP.variableposition_id = FE.variableposition_id\n"
													   + "INNER JOIN wres.Variable V\n"
													   + "	ON V.variable_id = VP.variable_id\n"
													   + "INNER JOIN wres.Ensemble E\n"
													   + "	ON E.ensemble_id = FE.ensemble_id\n"
													   + "WHERE FE.forecast_id = %d\n"
													   + "	AND V.variable_name = '%s'\n"
													   + "	AND E.ensemble_name = '%s';";
	
	private final String save_measurement_script = "INSERT INTO wres.measurementunit (unit_name)\n"
												 + "SELECT '%s'\n"
												 + "WHERE NOT EXISTS (\n"
												 + "	SELECT 1\n"
												 + "	FROM wres.measurementunit\n"
												 + "	WHERE unit_name = '%s'\n"
												 + ");";
	
	private final String variable_exists_script = "SELECT EXISTS (\n"
												+ "		SELECT 1\n"
												+ "		FROM wres.Variable\n"
												+ "		WHERE variable_name = '%s'\n"
												+ ") AS variable_exists;";
}
