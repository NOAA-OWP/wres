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
final class NetCDFReader // TODO implement Closeable or AutoCloseable
{

    private final Logger LOGGER = LoggerFactory.getLogger( NetCDFReader.class );

    private final String NEWLINE = System.lineSeparator();

    private final String VERTICAL_SPACER = "  ----  ";

    // TODO: use a NetcdfFile as member rather than String
    private String path;

    // TODO: open the netCDF file on construction
    public NetCDFReader( String path )
    {
        this.path = path;
    }


    /**
     * Prints variable information to the terminal from the member netCDF file
     * @throws IOException when reading fails
     * @throws InvalidRangeException when an invalid read range is passed to
     * the netCDF library
     */

    public void output_variables() throws IOException, InvalidRangeException
    {
        try ( NetcdfFile ncfile = NetcdfFile.open( path ) )
        {
            List<Variable> observed_variables = ncfile.getVariables();
            LOGGER.info( "Information about this NetCDF: {}    {}",
                         NEWLINE, ncfile.getDetailInfo() );

            List<Attribute> global_attributes = ncfile.getGlobalAttributes();

            for ( Attribute global_attribute : global_attributes )
            {
                LOGGER.info( "An attribute is: {}{}    The values are:",
                             global_attribute.getFullName(), NEWLINE );

                for ( int i = 0; i < global_attribute.getLength(); ++i )
                {
                    LOGGER.info( "        {}", global_attribute.getValue( i )
                                                               .toString() );
                }
            }

            LOGGER.info( VERTICAL_SPACER );

            for ( Variable var : observed_variables )
            {
                LOGGER.info( "{}This variable is: {}",
                             NEWLINE, var.getDescription() );
                LOGGER.info( "    The short name is: {}", var.getShortName() );

                List<Dimension> dimensions = var.getDimensions();

                for ( Dimension dimension : dimensions )
                {
                    LOGGER.info( "    A dimension is: {}",
                                 dimension.getShortName() );
                    LOGGER.info( "        The length is: {}",
                                 dimension.getLength() );
                }

                LOGGER.info( "    {} has a rank of: {}",
                             var.getShortName(), var.getRank() );
                String unit = var.getUnitsString();
                LOGGER.info( "    It is measured in: {}", unit );
                LOGGER.info(
                        "    The name and dimensions of this variable are: {}",
                        var.getNameAndDimensions() );
                LOGGER.info( "    The dimensions are: {}",
                             var.getDimensionsString() );
                LOGGER.info( "    The datatype is: {}",
                             var.getDataType().toString() );
                LOGGER.info( "    The description is: {}",
                             var.getDescription() );
                LOGGER.info( "    Is coordinate Variable: {}",
                             var.isCoordinateVariable() );
                LOGGER.info( "    There are {} dimensions in this variable{}",
                             var.getDimensions().size(), NEWLINE );

                LOGGER.info( "{}Attributes:{}", NEWLINE, NEWLINE );
                List<Attribute> attributes = var.getAttributes();

                for ( Attribute attribute : attributes )
                {
                    LOGGER.info( "    This attribute is: {}",
                                 attribute.getFullName() );
                    LOGGER.info( "        The values are: " );

                    for ( int i = 0; i < attribute.getLength(); ++i )
                    {
                        LOGGER.info( "            {}",
                                     attribute.getValue( i ).toString() );
                    }
                }

                /* Method getAnnotations seems deprecated in cdm 5
                Map<String, Object> annotations = var.getAnnotations();
                
                if (annotations.size() > 0)
                {
                    System.out.print("\nThe annotations are:");
                    
                    for (String key : annotations.keySet())
                    {
                        System.out.print("    ");
                        System.out.print(key);
                        System.out.print(" : ");
                        System.out.println(annotations.get(key));
                    }
                }

                */
                LOGGER.info( VERTICAL_SPACER );

                if ( var.getDimensions().size() > 0 )
                {
                    int[] origin = new int[var.getDimensions().size()];
                    int[] size = new int[var.getDimensions().size()];

                    Arrays.fill( origin, 0 );

                    for ( int i = 0; i < var.getDimensions().size(); ++i )
                    {
                        Dimension dim = var.getDimension( i );
                        size[i] = Math.min( 5, dim.getLength() );
                    }

                    LOGGER.info( "Some data are: " );

                    int counter = 0;
                    Array data = var.read( origin, size );

                    while ( data.hasNext() )
                    {
                        LOGGER.info( "    {}", data.next() );

                        if ( unit != null
                             && !unit.equalsIgnoreCase( "null" ) )
                        {
                            LOGGER.info( " {}", unit );
                        }
                        counter = counter + 1;

                        if ( counter >= 5 )
                        {
                            LOGGER.info( VERTICAL_SPACER );
                            counter = 0;
                        }
                    }

                    if ( path.endsWith( ".gz" ) )
                    {
                        // Removes the unpacked netcdf file from the file system
                        Files.deleteIfExists( Paths.get( path.replaceAll( ".gz",
                                                                          "" ) ) );
                    }

                    LOGGER.info( VERTICAL_SPACER );
                }
            }
        }
    }


    /**
     * prints data from an indicated location for a variable
     * @param variable_name The name of the variable to retrieve data from
     * @param args coordinates for finding variable values
     * @throws IOException when reading the netCDF file fails
     * @throws InvalidRangeException when an invalid range is passed to the
     * netCDF library
     */

    public void printQuery( String variable_name, int... args )
            throws IOException, InvalidRangeException
    {
        try ( NetcdfFile nc = NetcdfFile.open( path ) )
        {
            Variable var = nc.findVariable( variable_name );
            int[] origin = new int[var.getRank()];
            Arrays.fill( origin, 0 );
            int[] size = new int[var.getRank()];
            Arrays.fill( size, 1 );

            System.arraycopy( args,
                              0,
                              origin,
                              0,
                              Math.min( args.length, var.getRank() ) );

            LOGGER.info( "{}", var.read( origin, size ).getObject( 0 ) );

            if ( var.getUnitsString() != null && !var.getUnitsString()
                                                     .equalsIgnoreCase( "null" ) )
            {
                LOGGER.info( " {}", var.getUnitsString() );
            }
            LOGGER.info( "  -----  " );
        }
    }
}
