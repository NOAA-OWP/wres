package wres.io.writing.netcdf;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import wres.util.Collections;

public class NetCDFCopier implements Closeable
{
    private static final Logger
            LOGGER = LoggerFactory.getLogger( NetCDFCopier.class );

    private static final NetcdfFileWriter.Version NETCDF_VERSION = NetcdfFileWriter.Version.netcdf3;

    /**
     * NetCDF Attributes that are acceptable to use. They are currently
     * restricted to NetCDF-3, since NetCDF-4 cannot be written in Java
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

    private final String fromFileName;
    private final String targetFileName;

    private Collection<Variable> variablesToCopy;
    private Collection<Dimension> dimensionsToCopy;

    private NetcdfFile source;
    private NetcdfFileWriter writer;

    public NetCDFCopier( final String fromFilename, final String targetFileName )
            throws IOException
    {
        this.fromFileName = fromFilename;
        this.targetFileName = targetFileName;
        this.copyGlobalAttributes();
        this.copyDimensions();
        this.copyVariables();
    }

    public NetcdfFileWriter write() throws IOException
    {
        this.getWriter().create();
        try
        {
            this.copyOriginalData();
        }
        catch (  InvalidRangeException e )
        {
            throw new IOException( "Data from the original file could not be "
                                   + "copied into the new version.", e );
        }

        return this.getWriter();
    }

    private NetcdfFile getSource() throws IOException
    {
        if (this.source == null)
        {
            this.source = NetcdfFile.open( this.fromFileName );
        }
        return this.source;
    }

    private NetcdfFileWriter getWriter() throws IOException
    {
        if (this.writer == null)
        {
            Files.deleteIfExists( Paths.get(this.targetFileName ));
            this.writer = NetcdfFileWriter.createNew( NETCDF_VERSION,
                                                      this.targetFileName );
            this.writer.setFill( true );
            this.writer.setLargeFile( true );
        }
        return this.writer;
    }

    private Collection<Variable> getVariablesToCopy() throws IOException
    {
        if (this.variablesToCopy == null)
        {
            this.variablesToCopy = new ArrayList<>(  );
            for (Variable variable : this.getSource().getVariables())
            {
                if (variable.isCoordinateVariable())
                {
                    LOGGER.debug( "Planning to copy '{}' since it is a coordinate variable.",
                                 variable.getShortName() );
                    this.variablesToCopy.add(variable);
                }
                else if (variable.getDimensions().isEmpty())
                {
                    LOGGER.debug("Planning to copy '{}' since it is a scalar variable.",
                                variable.getShortName());
                    this.variablesToCopy.add(variable);
                }
            }
        }
        return this.variablesToCopy;
    }

    private  Collection<Dimension> getDimensionsToCopy() throws IOException
    {
        if (this.dimensionsToCopy == null)
        {
            this.dimensionsToCopy = new HashSet<>();

            this.getVariablesToCopy().forEach(
                    variable -> this.dimensionsToCopy.addAll(variable.getDimensions())
            );
        }

        return this.dimensionsToCopy;
    }

    private void copyDimensions() throws IOException
    {
        for (Dimension dimension : this.getDimensionsToCopy())
        {
            // TODO: What do we do when there are groups?
            this.getWriter().addDimension(null,
                                          dimension.getShortName(),
                                          dimension.getLength(),
                                          dimension.isUnlimited(),
                                          dimension.isVariableLength());
        }
    }

    private void copyGlobalAttributes() throws IOException
    {
        for (Attribute globalAttribute : this.getSource().getGlobalAttributes())
        {
            if (!Collections.in(globalAttribute.getDataType(), ACCEPTABLE_ATTRIBUTE_DATATYPES))
            {
                LOGGER.debug( "The global attribute '{}' will not be copied "
                             + "because the '{}' data type is not supported.",
                             globalAttribute.getShortName(),
                             globalAttribute.getDataType() );
                continue;
            }

            if (Collections.in( globalAttribute.getDataType(), NUMERIC_ATTRIBUTE_TYPES))
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
        for (Variable originalVariable : this.getVariablesToCopy())
        {
            List<Dimension> variableDimensions = new ArrayList<>(  );

            for (Dimension dimension : originalVariable.getDimensions())
            {
                variableDimensions.add( this.getWriter().findDimension( dimension.getShortName() ) );
            }

            Variable newVariable = this.getWriter()
                                       .addVariable(
                                               originalVariable.getShortName(),
                                               originalVariable.getDataType(),
                                               variableDimensions
                                       );

            for (Attribute originalAttribute : originalVariable.getAttributes())
            {
                if (!Collections.in(originalAttribute.getDataType(), ACCEPTABLE_ATTRIBUTE_DATATYPES))
                {
                    LOGGER.debug( "The attribute '{}' will not be copied to '{}' "
                                 + "because the '{}' data type is not supported.",
                                 originalAttribute.getShortName(),
                                 originalVariable.getShortName(),
                                 originalAttribute.getDataType() );
                    continue;
                }

                if (Collections.in(originalAttribute.getDataType(), NUMERIC_ATTRIBUTE_TYPES))
                {
                    newVariable.addAttribute( new Attribute( originalAttribute.getShortName(), originalAttribute.getNumericValue() ) );
                }
                else
                {
                    newVariable.addAttribute( new Attribute( originalAttribute.getShortName(), originalAttribute.getStringValue() ) );
                }
            }
        }
    }

    private void copyOriginalData() throws IOException, InvalidRangeException
    {
        if (this.getWriter().isDefineMode())
        {
            this.getWriter().create();
        }

        for (Variable originalVariable : this.getVariablesToCopy())
        {
            Array values = originalVariable.read();

            this.getWriter().write( originalVariable.getShortName(), values );
        }
    }

    public void addDimension(String name, int length, boolean isUnlimited, boolean isVariableLength) throws IOException
    {
        Dimension preexistingDimension = this.getWriter().findDimension( name );
        if(preexistingDimension != null)
        {
            preexistingDimension.setLength( length );
            preexistingDimension.setUnlimited( isUnlimited );
            preexistingDimension.setVariableLength( isVariableLength );
        }
        else
        {
            this.getWriter().addDimension( name, length, isUnlimited, isVariableLength );
        }
    }

    private void addGlobalAttribute(Attribute attribute) throws IOException
    {
        if (Collections.in(attribute.getDataType(), ACCEPTABLE_ATTRIBUTE_DATATYPES))
        {
            if (this.getWriter().findGlobalAttribute( attribute.getShortName() ) != null)
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

    public void addGlobalAttribute(String name, Number value) throws IOException
    {
        Attribute newAttribute = new Attribute( name, value );
        this.addGlobalAttribute( newAttribute );
    }

    public void addGlobalAttribute(String name, String value) throws IOException
    {
        Attribute newAttribute = new Attribute(name, value);
       this.addGlobalAttribute( newAttribute );
    }

    void addVariable( String name,
                             DataType dataType,
                             List<String> dimensionNames,
                             Map<String, Object> attributes )
            throws IOException
    {
        List<Dimension> dimensions = null;

        if (dimensionNames != null && !dimensionNames.isEmpty())
        {
            dimensions = new ArrayList<>(  );

            for (String dimensionName : dimensionNames )
            {
                Dimension foundDimension = this.getWriter().findDimension( dimensionName );

                if (foundDimension == null)
                {
                    throw new IllegalArgumentException( "The dimension '" + dimensionName + "' does not exist." );
                }

                dimensions.add( foundDimension );
            }
        }

        this.getWriter().addVariable( name, dataType, dimensions );

        this.getWriter().addVariableAttribute( name, "_FillValue", -999 );
        this.getWriter().addVariableAttribute( name, "missing_value", -999 );

        Attribute validRange = new Attribute( "valid_range", Arrays.asList(-100000, 100000) );

        this.getWriter().addVariableAttribute( name, validRange );

        Attribute chunkSizes = new Attribute( "_ChunkSizes", 905633 );

        this.getWriter().addVariableAttribute( name, chunkSizes );

        for (Map.Entry<String, Object> keyValue : attributes.entrySet())
        {
            if (keyValue.getValue() instanceof Number)
            {
                this.getWriter().addVariableAttribute( name, keyValue.getKey(),
                                                       (Number)keyValue.getValue() );
            }
            else
            {
                this.getWriter().addVariableAttribute( name, keyValue.getKey(),
                                                       String.valueOf(keyValue.getValue()) );
            }
        }
    }

    public boolean isGridded() throws IOException
    {
        boolean hasX = Collections.exists(
                this.dimensionsToCopy,
                dimension -> dimension.getShortName().equalsIgnoreCase( "x" )
        );
        hasX = hasX || this.getWriter().findVariable( "x" ) != null;

        boolean hasY = Collections.exists(
                this.dimensionsToCopy,
                dimension -> dimension.getShortName().equalsIgnoreCase( "y" )
        );
        hasY = hasY || this.getWriter().findVariable( "y" ) != null;

        return hasX && hasY;
    }

    public List<String> getMetricDimensionNames() throws IOException
    {
        List<String> dimensionList = new ArrayList<>(  );

        if (this.isGridded())
        {
            dimensionList.add( "y" );
            dimensionList.add("x");
        }
        else
        {
            // TODO: find a way to make the vector dimension name programatic and not hard coded
            dimensionList.add("feature_id");
        }

        return dimensionList;
    }

    @Override
    public void close() throws IOException
    {
        if (this.writer != null)
        {
            this.writer.flush();
            this.writer.close();
        }

        if (this.source != null)
        {
            this.source.close();
        }
    }
}
