package wres.writing.netcdf;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

import wres.reading.netcdf.Netcdf;

/**
 * Copies Netcdf data.
 */
public class NetCDFCopier implements Closeable
{
    private static final Logger
            LOGGER = LoggerFactory.getLogger( NetCDFCopier.class );

    private static final NetcdfFileWriter.Version NETCDF_VERSION = NetcdfFileWriter.Version.netcdf3;

    /**
     * Netcdf Attributes that are acceptable to use. They are currently
     * restricted to Netcdf-3, since Netcdf-4 cannot be written in Java
     */
    private static final DataType[] ACCEPTABLE_ATTRIBUTE_DATATYPES = new DataType[] {
            DataType.BYTE,
            DataType.CHAR,
            DataType.STRING,
            DataType.SHORT,
            DataType.INT,
            DataType.FLOAT,
            DataType.DOUBLE
    };

    private static final DataType[] NUMERIC_ATTRIBUTE_TYPES = new DataType[] {
            DataType.BYTE,
            DataType.SHORT,
            DataType.INT,
            DataType.LONG,
            DataType.FLOAT,
            DataType.DOUBLE,
            DataType.UBYTE,
            DataType.USHORT,
            DataType.UINT,
            DataType.ULONG
    };
    private static final String ANALYSIS_TIME = "analysis_time";
    private final String fromFileName;
    private final String targetFileName;
    private final ZonedDateTime analysisTime;

    private NetcdfFile source;
    private NetcdfFileWriter writer;

    /**
     * Creates an instance.
     * @param fromFilename the source file name
     * @param targetFileName the target file name
     * @param analysisTime the analysis time
     * @throws IOException if the data could not be copied
     */
    public NetCDFCopier( final String fromFilename, final String targetFileName, final ZonedDateTime analysisTime )
            throws IOException
    {
        this.fromFileName = fromFilename;
        this.targetFileName = targetFileName;
        this.analysisTime = analysisTime;
        this.copyGlobalAttributes();
        this.copyDimensions();
        this.copyVariables();

        // Make sure that there's an analysis time variable to tie everything together
        if ( this.getWriter().findVariable( ANALYSIS_TIME ) == null )
        {
            if ( this.getWriter().findDimension( ANALYSIS_TIME ) == null )
            {
                this.getWriter().addDimension( ANALYSIS_TIME, 1 );
            }

            Map<String, Object> attributes = new TreeMap<>();
            attributes.put( "units", "minutes since 1970-01-01 00:00:00 UTC" );
            attributes.put( "standard_name", ANALYSIS_TIME );
            attributes.put( "long_name", "lead hour" );

            List<String> dimensionNames = new ArrayList<>();
            dimensionNames.add( ANALYSIS_TIME );

            this.addVariable( ANALYSIS_TIME, DataType.INT, dimensionNames, attributes );
        }
    }

    /**
     * Creates a writer to write the data.
     * @return a writer instance
     * @throws IOException if the writer could not be created
     */
    public NetcdfFileWriter write() throws IOException
    {
        this.getWriter().create();
        try
        {
            this.copyOriginalData();
        }
        catch ( InvalidRangeException e )
        {
            throw new IOException( "Data from the original file could not be "
                                   + "copied into the new version.", e );
        }

        return this.getWriter();
    }

    private NetcdfFile getSource() throws IOException
    {
        if ( this.source == null )
        {
            LOGGER.debug( "Opening the template at {} to copy", this.fromFileName );
            this.source = NetcdfFiles.open( this.fromFileName );
        }
        return this.source;
    }

    private NetcdfFileWriter getWriter() throws IOException
    {
        if ( this.writer == null )
        {
            LOGGER.debug( "Removing a previous version of {} if it already exists.", this.targetFileName );
            Files.deleteIfExists( Paths.get( this.targetFileName ) );

            LOGGER.debug( "Created {}", this.targetFileName );
            this.writer = NetcdfFileWriter.createNew( NETCDF_VERSION,
                                                      this.targetFileName );
            this.writer.setFill( true );
            this.writer.setLargeFile( true );
        }
        return this.writer;
    }

    private void copyDimensions() throws IOException
    {
        for ( Dimension dimension : this.getSource().getDimensions() )
        {
            this.getWriter().addDimension( null,
                                           dimension.getShortName(),
                                           dimension.getLength(),
                                           dimension.isUnlimited(),
                                           dimension.isVariableLength() );
        }
    }

    private void copyGlobalAttributes() throws IOException
    {
        for ( Attribute globalAttribute : this.getSource().getGlobalAttributes() )
        {
            boolean matched = Arrays.stream( ACCEPTABLE_ATTRIBUTE_DATATYPES )
                                    .anyMatch( next -> next.equals( globalAttribute.getDataType() ) );
            if ( !matched )
            {
                LOGGER.debug( "The global attribute '{}' will not be copied "
                              + "because the '{}' data type is not supported.",
                              globalAttribute.getShortName(),
                              globalAttribute.getDataType() );
                continue;
            }

            boolean matchedNumeric = Arrays.stream( NUMERIC_ATTRIBUTE_TYPES )
                                           .anyMatch( next -> next.equals( globalAttribute.getDataType() ) );
            if ( matchedNumeric )
            {
                this.getWriter().addGlobalAttribute( globalAttribute.getShortName(),
                                                     globalAttribute.getNumericValue() );
            }
            else
            {
                this.getWriter().addGlobalAttribute( globalAttribute.getShortName(),
                                                     globalAttribute.getStringValue() );
            }
        }
    }

    private void copyVariables() throws IOException
    {
        for ( Variable originalVariable : this.getSource().getVariables() )
        {
            List<Dimension> variableDimensions = new ArrayList<>();

            for ( Dimension dimension : originalVariable.getDimensions() )
            {
                variableDimensions.add( this.getWriter().findDimension( dimension.getShortName() ) );
            }

            Variable newVariable = this.getWriter()
                                       .addVariable(
                                               originalVariable.getShortName(),
                                               originalVariable.getDataType(),
                                               variableDimensions
                                       );


            for ( Attribute originalAttribute : originalVariable.getAttributes() )
            {
                boolean matched = Arrays.stream( ACCEPTABLE_ATTRIBUTE_DATATYPES )
                                        .anyMatch( next -> next.equals( originalAttribute.getDataType() ) );

                if ( !matched )
                {
                    LOGGER.debug( "The attribute '{}' will not be copied to '{}' "
                                  + "because the '{}' data type is not supported.",
                                  originalAttribute.getShortName(),
                                  originalVariable.getShortName(),
                                  originalAttribute.getDataType() );
                    continue;
                }

                if ( originalVariable.getShortName().equalsIgnoreCase( "time" ) && originalAttribute.getShortName()
                                                                                                    .equals( "units" ) )
                {
                    newVariable.addAttribute(
                            new Attribute( "time",
                                           "minutes from " + Netcdf.getStandardDateFormat().format( this.analysisTime )
                            )
                    );
                }
                else
                {
                    Attribute new_attribute = new Attribute( originalAttribute.getShortName(), originalAttribute );
                    newVariable.addAttribute( new_attribute );
                }
            }
        }
    }

    private void copyOriginalData() throws IOException, InvalidRangeException
    {
        if ( this.getWriter().isDefineMode() )
        {
            this.getWriter().create();
        }

        for ( Variable originalVariable : this.getSource().getVariables() )
        {
            LOGGER.debug( "Copying over all {} data from {} to {}",
                          originalVariable.getShortName(),
                          this.fromFileName,
                          this.targetFileName );
            Array values = originalVariable.read();

            this.getWriter().write( originalVariable.getShortName(), values );
        }
        LOGGER.debug( "Done copying all variable data from the template." );
    }

    private void addGlobalAttribute( Attribute attribute ) throws IOException
    {
        boolean matched = Arrays.stream( ACCEPTABLE_ATTRIBUTE_DATATYPES )
                                .anyMatch( next -> next.equals( attribute.getDataType() ) );
        if ( matched )
        {
            if ( this.getWriter().findGlobalAttribute( attribute.getShortName() ) != null )
            {
                LOGGER.debug( "The preexisting global attribute '{}' will be "
                              + "overwritten in '{}'.",
                              attribute.getShortName(),
                              this.targetFileName );
                this.getWriter().deleteGlobalAttribute( attribute.getShortName() );
            }

            this.getWriter().addGlobalAttribute( attribute );
        }
        else
        {
            throw new IllegalArgumentException( "The data type of '" +
                                                attribute.getDataType() +
                                                "' is not supported." );
        }
    }

    void addVariable( String name,
                      DataType dataType,
                      List<String> dimensionNames,
                      Map<String, Object> attributes )
            throws IOException
    {
        List<Dimension> dimensions = null;

        if ( dimensionNames != null && !dimensionNames.isEmpty() )
        {
            dimensions = new ArrayList<>();

            for ( String dimensionName : dimensionNames )
            {
                Dimension foundDimension = this.getWriter().findDimension( dimensionName );

                if ( foundDimension == null )
                {
                    throw new IllegalArgumentException( "The dimension '" + dimensionName + "' does not exist." );
                }

                dimensions.add( foundDimension );
            }
        }

        this.getWriter().addVariable( name, dataType, dimensions );

        this.getWriter().addVariableAttribute( name, "_FillValue", -999.0F );
        this.getWriter().addVariableAttribute( name, "missing_value", -999.0F );

        Attribute validRange = new Attribute( "valid_range", Arrays.asList( -900.0F, 900.0F ) );

        this.getWriter().addVariableAttribute( name, validRange );

        Attribute chunkSizes = new Attribute( "_ChunkSizes", 905633 );

        this.getWriter().addVariableAttribute( name, chunkSizes );

        Variable coordinateSystem = this.getWriter().findVariable( "ProjectionCoordinateSystem" );
        if ( coordinateSystem != null )
        {
            this.getWriter()
                .addVariableAttribute( name, new Attribute( "grid_mapping", "ProjectionCoordinateSystem" ) );

            Attribute esriPEString = coordinateSystem.findAttribute( "esri_pe_string" );

            if ( esriPEString != null && esriPEString.isString() )
            {
                this.getWriter()
                    .addVariableAttribute( name,
                                           new Attribute( "esri_pe_string",
                                                          Objects.requireNonNull( esriPEString.getStringValue() ) ) );
            }

            Attribute proj4 = coordinateSystem.findAttribute( "proj4" );

            if ( proj4 != null && proj4.isString() )
            {
                this.getWriter().addVariableAttribute( name,
                                                       new Attribute( "proj4",
                                                                      Objects.requireNonNull( proj4.getStringValue() ) ) );
            }

            this.getWriter().addVariableAttribute( name, new Attribute( "coordinates", "time lat lon" ) );
        }

        for ( Entry<String, Object> keyValue : attributes.entrySet() )
        {
            if ( keyValue.getValue() instanceof Number number )
            {
                this.getWriter().addVariableAttribute( name, keyValue.getKey(),
                                                       number );
            }
            else
            {
                this.getWriter().addVariableAttribute( name, keyValue.getKey(),
                                                       String.valueOf( keyValue.getValue() ) );
            }
        }
    }

    /**
     * @return whether the data is gridded
     * @throws IOException if the data could not be examined
     */
    public boolean isGridded() throws IOException
    {
        boolean hasX = this.getSource()
                           .getDimensions()
                           .stream()
                           .anyMatch( dimension -> dimension.getShortName()
                                                            .equalsIgnoreCase( "x" ) );

        hasX = hasX || this.getWriter().findVariable( "x" ) != null;

        boolean hasY = this.getSource()
                           .getDimensions()
                           .stream()
                           .anyMatch( dimension -> dimension.getShortName()
                                                            .equalsIgnoreCase( "y" ) );
        hasY = hasY || this.getWriter().findVariable( "y" ) != null;

        return hasX && hasY;
    }

    /**
     * @return the metric dimension names
     * @throws IOException if it could not be determined whether the data is gridded
     */
    public List<String> getMetricDimensionNames() throws IOException
    {
        List<String> dimensionList = new ArrayList<>();

        if ( this.isGridded() )
        {
            dimensionList.add( "y" );
            dimensionList.add( "x" );
        }
        else
        {
            dimensionList.add( "lid" );
        }

        return dimensionList;
    }

    @Override
    public void close() throws IOException
    {
        if ( this.writer != null )
        {
            this.writer.flush();
            this.writer.close();
        }

        if ( this.source != null )
        {
            this.source.close();
        }
    }
}
