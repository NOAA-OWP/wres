/**
 * 
 */
package wres.util;

import java.util.List;
import java.io.IOException;
import ucar.nc2.Variable;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.ma2.Array;
import ucar.ma2.DataType;
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
			int x = 3000;
			int y = 3000;
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
				
				System.out.println("");
				
				if (var.getDimensions().size() == 3 && var.getDataType() == DataType.INT) {
                    int[] origin = new int[] {0, x, y};
                    int[] size = new int[] {1, 5, 5};
                    Array data = var.read();//origin, size);
                    System.out.println("Some data are: ");
                    int nullValue = data.getInt(0);
                    //System.out.println(data.getObject(0));
                    try {
                		int counter = 0;
                    	Array section = data.section(origin, size);
                    	while (section.hasNext())
                    	{
                    		int next_val = section.nextInt();
                    		if (nullValue != next_val)
                    		{
                    			//System.out.print("\tAn acceptable value is: ");
                    			System.out.print("\t");
                    			System.out.print(next_val);
                    			System.out.print(" " + unit);
                    			counter = counter + 1;
                    			if (counter >= 5) {
                    				System.out.println("");
                    				counter = 0;
                    			}                    			
                    		}                    		
                    	}
						//System.out.println(data.section(origin, size));
					} catch (InvalidRangeException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                    System.out.println("");
                }
			}
		} 
		catch (IOException error) 
		{
            System.out.println("Something went wrong while reading file named " + path + " : " + error);
			error.printStackTrace();
		}
	}
	
	private String get_path()
	{
		return path;
	}
	
	private String path = "";

}
