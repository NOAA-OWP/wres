/**
 * 
 */
package wres.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import ucar.nc2.Variable;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;

/**
 * @author ctubbs
 *
 */
public final class NetCDFReader {

	/**
	 * 
	 */
	public NetCDFReader(String path) {
		this.path = path;
	}
	
	public void output_variables()
	{
		try (NetcdfFile ncfile = NetcdfFile.open(get_path())) 
		{
			List<Variable> observed_variables = ncfile.getVariables();
			System.out.print("Information about this NetCDF: \n\t");
			System.out.println(ncfile.getDetailInfo());
			
			List<Attribute> global_attributes = ncfile.getGlobalAttributes();
			for (int index = 0; index < global_attributes.size(); ++index)
			{
				Attribute global_attribute = global_attributes.get(index);
				System.out.println("An attribute is: " + global_attribute.getFullName());
				System.out.println("\tThe values are: ");
				
				for (int i = 0; i < global_attribute.getLength(); ++i)
				{
					System.out.print("\t\t");
					System.out.println(global_attribute.getValue(i));
				}
			}
			System.out.println("");

			for (Variable var : observed_variables)
			{
				System.out.println("\nThis variable is: " + var.getDescription());
				System.out.println("\tThe short name is: " + var.getShortName());
				List<Dimension> dimensions = var.getDimensions();
				for (Dimension dimension : dimensions)
				{
					System.out.println("\tA dimension is: " + dimension.getShortName());
					System.out.print("\t\tThe length is: ");
					System.out.println(dimension.getLength());
				}
				System.out.println("\t" + var.getShortName() + " has a rank of: " + String.valueOf(var.getRank()));
				String unit = var.getUnitsString();
				System.out.println("\tIt is measured in: " + unit);
				System.out.println("\tThe name and dimensions of this variable are: " + var.getNameAndDimensions());
				System.out.println("\tThe dimensions are: " + var.getDimensionsString());
				System.out.print("\tThe datatype is: ");
				System.out.println(var.getDataType());
				System.out.println("\tThe description is: " + var.getDescription());
				System.out.print("\tIs coordinate Variable: ");
				System.out.println(var.isCoordinateVariable());
				System.out.print("\tThere are ");
				System.out.print(var.getDimensions().size());
				System.out.println(" dimensions in this variable.\n");
				System.out.println("\nAttributes:\n");
				List<Attribute> attributes = var.getAttributes();
				for (Attribute attribute : attributes)
				{
					System.out.println("\tThis attribute is: " + attribute.getFullName());
					System.out.println("\t\tThe values are: ");
					
					for (int i = 0; i < attribute.getLength(); ++i)
					{
						System.out.print("\t\t\t");
						System.out.println(attribute.getValue(i));
					}
				}
				
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
                			System.out.print(" " + unit);
                			counter = counter + 1;
                			if (counter >= 5) {
                				System.out.println();
                				counter = 0;
                			}                    		
                    	}
					} catch (InvalidRangeException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                    
                    if (get_path().endsWith(".gz"))
                    {
	                    // Removes the unpacked netcdf file from the file system
	                    Files.deleteIfExists(Paths.get(get_path().replaceAll(".gz", "")));
                    }
                    
                    System.out.println();
                }
			}
		} 
		catch (IOException error) 
		{
            System.out.println("Something went wrong while reading file named " + path + " : " + error);
			error.printStackTrace();
		}
	}
	
	public void print_query(String variable_name, int... args)
	{
			NetcdfFile nc;
			try {
				nc = NetcdfFile.open(get_path());			
				Variable var = nc.findVariable(variable_name);
				int[] origin = new int[var.getRank()];
				Arrays.fill(origin, 0);
				int[] size = new int[var.getRank()];
				Arrays.fill(size, 1);
				
				for (int i = 0; i < Math.min(args.length, var.getRank()); ++i)
				{
					origin[i] = args[i];
				}
				
				System.out.print(var.read(origin, size).getObject(0));
				if (var.getUnitsString() != "null")
				{
					System.out.print(" " + var.getUnitsString());
				}
				System.out.println("");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidRangeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}
	
	private String get_path()
	{
		return path;
	}
	
	private String path = "";

}
