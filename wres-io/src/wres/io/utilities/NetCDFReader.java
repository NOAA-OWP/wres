/**
 * 
 */
package wres.io.utilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import wres.util.Strings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

@Deprecated
/**
 * @author Christopher Tubbs
 * The initial draft of the NetCDF reader
 * Useful for sample code
 */
final class NetCDFReader {

	private final Logger LOGGER = LoggerFactory.getLogger(NetCDFReader.class);

	/**
	 * Constructor
	 */
	public NetCDFReader(String path) {
		this.path = path;
	}
	
	/**
	 * Prints variable information to the terminal
	 */
	public void output_variables()
	{
		try (NetcdfFile ncfile = NetcdfFile.open(path)) 
		{
			List<Variable> observed_variables = ncfile.getVariables();
			LOGGER.info("Information about this NetCDF: \n\t");
			LOGGER.info(ncfile.getDetailInfo());
			
			List<Attribute> global_attributes = ncfile.getGlobalAttributes();
			for (Attribute global_attribute : global_attributes) {
				LOGGER.info("An attribute is: " + global_attribute.getFullName());
				LOGGER.info("\tThe values are: ");

				for (int i = 0; i < global_attribute.getLength(); ++i) {
					LOGGER.info("\t\t{}", global_attribute.getValue(i).toString());
				}
			}
			LOGGER.info("");

			for (Variable var : observed_variables)
			{
				LOGGER.info("\nThis variable is: " + var.getDescription());
				LOGGER.info("\tThe short name is: " + var.getShortName());
				List<Dimension> dimensions = var.getDimensions();
				for (Dimension dimension : dimensions)
				{
					LOGGER.info("\tA dimension is: " + dimension.getShortName());
					LOGGER.info("\t\tThe length is: {}", String.valueOf(dimension.getLength()));
				}
				LOGGER.info("\t" + var.getShortName() + " has a rank of: " + String.valueOf(var.getRank()));
				String unit = var.getUnitsString();
				LOGGER.info("\tIt is measured in: " + unit);
				LOGGER.info("\tThe name and dimensions of this variable are: " + var.getNameAndDimensions());
				LOGGER.info("\tThe dimensions are: " + var.getDimensionsString());
				LOGGER.info("\tThe datatype is: {}", var.getDataType().toString());
				LOGGER.info("\tThe description is: " + var.getDescription());
				LOGGER.info("\tIs coordinate Variable: {}", String.valueOf(var.isCoordinateVariable()));
				LOGGER.info("\tThere are {} dimensions in this variable\n", String.valueOf(var.getDimensions().size()));
				LOGGER.info("\nAttributes:\n");
				List<Attribute> attributes = var.getAttributes();
				for (Attribute attribute : attributes)
				{
					LOGGER.info("\tThis attribute is: " + attribute.getFullName());
					LOGGER.info("\t\tThe values are: ");
					
					for (int i = 0; i < attribute.getLength(); ++i)
					{
						LOGGER.info("\t\t\t{}", attribute.getValue(i).toString());
					}
				}

				/* Method getAnnotations seems deprecated in cdm 5
				Map<String, Object> annotations = var.getAnnotations();
				
				if (annotations.size() > 0)
				{
					System.out.print("\nThe annotations are:");
					
					for (String key : annotations.keySet())
					{
						System.out.print("\t");
						System.out.print(key);
						System.out.print(" : ");
						System.out.println(annotations.get(key));
					}
				}

				*/
				System.out.println("");
				if (var.getDimensions().size() > 0) {
                    int[] origin = new int[var.getDimensions().size()];
                    int[] size = new int[var.getDimensions().size()];
                    
                    Arrays.fill(origin, 0);
                    
                    for (int i = 0; i < var.getDimensions().size(); ++i)
                    {
                    	Dimension dim = var.getDimension(i);
                    	size[i] = Math.min(5, dim.getLength());
                    }
                    
                    System.out.println("Some data are: ");

                    try {
                		int counter = 0;
                        Array data = var.read(origin, size);
                    	while (data.hasNext())
                    	{
                			System.out.print("\t");
                			System.out.print(data.next());

                			if (unit != null && !unit.equalsIgnoreCase("null")) {
								System.out.print(" " + unit);
							}
                			counter = counter + 1;
                			if (counter >= 5) {
                				LOGGER.info("");
                				counter = 0;
                			}                    		
                    	}
					} catch (InvalidRangeException e) {
						LOGGER.error(Strings.getStackTrace(e));
					}
                    
                    if (path.endsWith(".gz"))
                    {
	                    // Removes the unpacked netcdf file from the file system
	                    Files.deleteIfExists(Paths.get(path.replaceAll(".gz", "")));
                    }
                    
                    LOGGER.info("");
                }
			}
		} 
		catch (IOException error) 
		{
            LOGGER.error("Something went wrong while reading file named " + path + " : " + error);
			LOGGER.error(Strings.getStackTrace(error));
		}
	}
	
	/**
	 * prints data from an indicated location for a variable
	 * @param variable_name The name of the variable to retrieve data from
	 * @param args coordinates for finding variable values 
	 */
	public void printQuery (String variable_name, int... args)
	{
			try (NetcdfFile nc = NetcdfFile.open(path))
			{
				Variable var = nc.findVariable(variable_name);
				int[] origin = new int[var.getRank()];
				Arrays.fill(origin, 0);
				int[] size = new int[var.getRank()];
				Arrays.fill(size, 1);

				System.arraycopy(args, 0, origin, 0, Math.min(args.length, var.getRank()));
				
				System.out.print(var.read(origin, size).getObject(0));
				if (var.getUnitsString() != null && !var.getUnitsString().equalsIgnoreCase("null"))
				{
					System.out.print(" " + var.getUnitsString());
				}
				System.out.println("");
			} catch (IOException | InvalidRangeException e) {
				LOGGER.error(Strings.getStackTrace(e));
			}

	}
	
	private String path = "";

}
