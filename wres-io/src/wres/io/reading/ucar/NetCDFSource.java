package wres.io.reading.ucar;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import ucar.ma2.Array;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import wres.io.concurrency.Executor;
//import wres.io.grouping.AssociatedPairs;
import wres.io.reading.BasicSource;
import wres.util.Collections;
import wres.io.utilities.Database;

/**
 * @author ctubbs
 *
 */
public class NetCDFSource extends BasicSource {

	private class ValueInserter implements Runnable
	{
		public ValueInserter(List<ForecastValue> parameters)
		{
			this.parameters = parameters;
			script_builder = new StringBuilder(insert_header);
		}

		@Override
		public void run() {
			boolean add_comma = false;
			
			for (ForecastValue parameter : parameters)
			{
				if (add_comma)
				{
					script_builder.append(",\n");
				}
				else
				{
					add_comma = true;
				}

				script_builder.append("(");
				script_builder.append(String.valueOf(parameter.getForecastEnsembleID()));
				script_builder.append(", ");
				script_builder.append(String.valueOf(parameter.getLead()));
				script_builder.append(", ");
				script_builder.append(String.valueOf(parameter.getForecastedValue()));
				script_builder.append(")");
			}
			
			script_builder.append(";");
			
			try {
				Database.execute(script_builder.toString());
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		
		private List<ForecastValue> parameters;
		private final String insert_header = "INSERT INTO wres.ForecastValue\n"
										   + "(\n"
										   + "		forecastensemble_id,\n"
										   + "		lead,\n"
										   + "		forecasted_value\n"
										   + ")\n"
										   + "VALUES\n";
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
				boolean exists = Database.getResult(String.format(variable_exists_script, variable_to_insert.getShortName()), "variable_exists");
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
					
					Integer variable_id = Database.getResult(script, "variable_id");

					
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
	public void save_observation() throws IOException
	{
		
	}
	
	@Override
	public void save_forecast() throws IOException
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
	
	protected void save_values() throws Exception
	{
		NetcdfFile source = get_source();
		int insert_limit = 10000;
		List<ForecastValue> values_to_insert;
		int dimension_count;
		Double found_value = null;
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
				
				values_to_insert = new ArrayList<>();
				loadVariablePositions(var.getShortName());
				
				int x_length = get_x_length(var);
				
				int y_position = -1;
				
				switch (dimension_count)
				{
				case 2:
					y_position = 1;
					break;
				case 3:
					y_position = 2;
				}
				
				int[] origin = new int[dimension_count];
				int[] shape = new int[dimension_count];

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
							
							if (!found_value.equals(fill_value))
							{
								values_to_insert.add(new ForecastValue(getPosition(current_index, 0), get_lead(), found_value));
							}
							current_index++;
						}

						Executor.execute(new ValueInserter(values_to_insert));
						values_to_insert = new ArrayList<>();
					}
				}
				
				if (values_to_insert.size() > 0)
				{
					System.out.println("Dispatching one last job to save values...");
				}
			}
		}
		Executor.complete();
	}
	
	// TODO: Retrieve the value from another location instead of trying to get the value
	// from an internal collection
	private int getPosition(int x_index, int y_index)
	{
		return 0; //this.variable_positions.get(x_index, y_index);
	}
	
	/**
	 * Populates the variablePositions collection with the position of each for forecast ensemble based on the variable name
	 * @param variable_name The name of the variable for whose variable positions to load
	 * @throws SQLException Any possible issue that was encountered upon trying to interact with the database
	 * 
	 * TODO: Reimplement without relying on the associated pairs structure
	 */
	private void loadVariablePositions(String variable_name) throws SQLException {
		System.out.println("Loading a new set of value positions for " + variable_name);
		//this.variable_positions = new AssociatedPairs<Integer, Integer, Integer>();
		Connection connection = null;
		ResultSet positionSet = null;
		
		try {
	        connection = Database.getConnection();
	        String variablePositionLoad = String.format(load_variablepositions_script, 
                                                        get_forecast_id(), 
                                                        variable_name, 
                                                        this.ensemble_name);
	        
	        positionSet = Database.getResults(connection, variablePositionLoad);
	        /*while (positionSet.next())
	        {
	            this.variable_positions.put(positionSet.getInt("x_position"), 
	                                        positionSet.getInt("y_position"), 
	                                        positionSet.getInt("forecastensemble_id"));
	        }*/
	        System.out.println("Positions loaded.");
		} finally {
		    if (positionSet != null) {
		        positionSet.close();
		    }
		    
		    if (connection != null) {
		        Database.returnConnection(connection);
		    }
		}		
	}
	
	private void save_variables() throws IOException
	{
		NetcdfFile source = get_source();

		for (Variable var : source.getVariables())
		{
			if (var.getDimensions().size() > 1 || var.getDimension(0).getLength() > 1)
			{
				
				Executor.execute(new VariableInserter(var));
			}
		}
	}
	
	private static int get_x_length(Variable var)
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
	
	private static int get_y_length(Variable var)
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
	
	protected Integer get_forecast_hour()
	{
		if (this.forecast_hour == null)
		{
			String description = Collections.find(get_file_parts(), (String possibility) -> {
				return possibility.endsWith("z");
			});
			
			description = description.replaceAll("\\D", "");
			this.forecast_hour = Integer.parseInt(description);
		}
		return this.forecast_hour;
	}
	
	private Integer get_lead()
	{
		if (this.lead == null)
		{
			String description = Collections.find(get_file_parts(), (String possibility) -> {
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
			this.range = Collections.find(get_file_parts(), (String possibility) -> {
				return possibility.endsWith("range") || possibility.endsWith("assim");
			});
		}
		return this.range;
	}
	
	protected String get_data_category()
	{
		if (this.data_category == null)
		{
			String category = Collections.find(get_file_parts(), (String possibility) -> {
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
			this.forecast_id = Database.getResult(String.format(get_forecast_id_script, 
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
	
	// TODO: Rely on supplier of variable position data rather than an internal collection
	//private AssociatedPairs<Integer, Integer, Integer> variable_positions = null;
	private String ensemble_name = "default";
	
	private String model_initialization_time;
	private Double missing_value = null;
	private Integer forecast_id = null;
	
	// TODO: DON'T SET UP SCRIPTS FOR STRING FORMATTING
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

    // TODO: DON'T SET UP SCRIPTS FOR STRING FORMATTING
	private final String get_forecast_id_script = "SELECT forecast_id\n"
												+ "FROM wres.Forecast F\n"
												+ "INNER JOIN wres.ForecastType T\n"
												+ "	ON T.forecasttype_id = F.forecasttype_id\n"
												+ "WHERE F.forecast_date = '%s'\n"
												+ "	AND T.type_name = '%s';";

    // TODO: DON'T SET UP SCRIPTS FOR STRING FORMATTING
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

    // TODO: DON'T SET UP SCRIPTS FOR STRING FORMATTING
	private final String get_variable_id_script = "SELECT variable_id\n"
												+ "FROM wres.Variable\n"
												+ "WHERE variable_name = '%s'\n"
												+ "		AND description = '%s';";

    // TODO: DON'T SET UP SCRIPTS FOR STRING FORMATTING
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

    // TODO: DON'T SET UP SCRIPTS FOR STRING FORMATTING
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

    // TODO: DON'T SET UP SCRIPTS FOR STRING FORMATTING
	private final String save_measurement_script = "INSERT INTO wres.measurementunit (unit_name)\n"
												 + "SELECT '%s'\n"
												 + "WHERE NOT EXISTS (\n"
												 + "	SELECT 1\n"
												 + "	FROM wres.measurementunit\n"
												 + "	WHERE unit_name = '%s'\n"
												 + ");";

    // TODO: DON'T SET UP SCRIPTS FOR STRING FORMATTING
	private final String variable_exists_script = "SELECT EXISTS (\n"
												+ "		SELECT 1\n"
												+ "		FROM wres.Variable\n"
												+ "		WHERE variable_name = '%s'\n"
												+ ") AS variable_exists;";
	
	private class ForecastValue implements Comparable<ForecastValue>
	{
	    public ForecastValue(int forecastEnsembleID, int lead, double forecastedValue)
	    {
	        this.forecastEnsembleID = forecastEnsembleID;
	        this.lead = lead;
	        this.forecastedValue = forecastedValue;
	    }

        @Override
        public int compareTo(ForecastValue other)
        {
            int equality;
            
            if (this.getForecastEnsembleID() < other.getForecastEnsembleID())
            {
                equality = -1;
            }
            else if (this.getForecastEnsembleID() > other.getForecastEnsembleID())
            {
                equality = 1;
            }
            else
            {
                equality = 0;
            }
            
            if (equality == 0)
            {
                if (this.getLead() < other.getLead())
                {
                    equality = -1;
                }
                else if (this.getLead() > other.getLead())
                {
                    equality = 1;
                }
                else
                {
                    equality = 0;
                }
            }
            
            if (equality == 0)
            {
                if (this.getForecastedValue() < other.getForecastedValue())
                {
                    equality = -1;
                }
                else if (this.getForecastedValue() > other.getForecastedValue())
                {
                    equality = 1;
                }
                else
                {
                    equality = 0;
                }
            }
            
            return equality;
        }
        
        public int getForecastEnsembleID()
        {
            return this.forecastEnsembleID;
        }
        
        public int getLead()
        {
            return this.lead;
        }
        
        public double getForecastedValue()
        {
            return this.forecastedValue;
        }
	    
        private int forecastEnsembleID;
        private int lead;
        private double forecastedValue;
	}
}
