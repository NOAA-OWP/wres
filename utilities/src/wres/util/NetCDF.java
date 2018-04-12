package wres.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Objects;
import java.util.regex.Pattern;
import java.nio.file.Paths;

/**
 * Created by ctubbs on 7/7/17.
 */
public final class NetCDF {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetCDF.class);

    // TODO: Maybe change this to support forcing data?
    private static final Pattern NWM_NAME_PATTERN = Pattern.compile(
            "^nwm\\.t\\d\\dz\\.(short|medium|long|analysis)_(range|assim)\\.[a-zA-Z]+(_rt)?(_\\d)?\\.(f\\d\\d\\d|tm\\d\\d)\\.conus\\.nc(\\.gz)?$"
    );

    private static final Pattern NWM_CATEGORY_PATTERN =
            Pattern.compile( "(?<=(assim|range)\\.)[a-zA-Z_\\d]+(?=\\.(tm\\d\\d|f\\d\\d\\d))" );

    private static final Pattern NETCDF_FILENAME_PATTERN = Pattern.compile( ".+\\.nc(\\.gz)?" );

    private static final Pattern SHORT_DATE_PATTERN = Pattern.compile("\\d{8}");

    public static class Ensemble
    {
        public Ensemble(String name, String qualifier, String tMinus)
        {
            this.name = name;
            this.qualifier = qualifier;
            this.tMinus = tMinus;
        }

        private final String name;
        private final String qualifier;
        private final String tMinus;

        public String getName()
        {
            return this.name;
        }

        public String getQualifier()
        {
            return this.qualifier;
        }

        public String getTMinus()
        {
            return this.tMinus;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( this.name, this.qualifier, this.tMinus);
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof Ensemble && this.hashCode() == obj.hashCode();
        }
    }

    private NetCDF() {}

    /**
     * Determines if a variable with the indicated short name exists within the NetCDF file
     * The provided "findVariable" function searches by full name, rather than short name. Since we only
     * care about the short name, this function had to be implemented.
     *
     * @param file The NetCDF file to look into
     * @param variableName The short name of the variable to find
     * @return A boolean indicating whether or not the file has the given variable name
     */
    public static boolean hasVariable (NetcdfFile file, String variableName)
    {
        return NetCDF.getVariable( file, variableName ) != null;
    }

    /**
     * Attempts to retrieve the variable with the given short name from within the passed in NetCDF file
     * @param file The file to search through
     * @param variableName The short name of the variable to retrieve
     * @return If the variable exists, the variable is retrieved. Otherwise, it is null.
     */
    public static Variable getVariable(NetcdfFile file, String variableName)
    {
        return file.findVariable( variableName );
    }


    public static Integer getLeadTime( NetcdfFile file)
    {
        int lead = Integer.MAX_VALUE;
        String initializedTime = NetCDF.getInitializedTime( file );
        String validTime = NetCDF.getValidTime( file );

        if (initializedTime != null && validTime != null)
        {

            LocalDateTime initialization = LocalDateTime.parse(initializedTime);
            LocalDateTime valid = LocalDateTime.parse(validTime);
            lead = ((Long)initialization.until(valid, TimeHelper.LEAD_RESOLUTION)).intValue();
        }
        else if (NetCDF.hasVariable( file, "time" ) && NetCDF.hasVariable( file, "reference_time" ))
        {
            Variable referenceTime = NetCDF.getVariable( file, "reference_time" );
            Variable time = NetCDF.getVariable( file, "time" );
            if (time.getDimensions().size() == 1 && referenceTime.getDimensions().size() == 1)
            {
                Integer reference = null;
                Integer valid = null;

                try
                {
                    Array referenceValues = referenceTime.read( new int[]{0}, new int[]{1} );
                    reference = referenceValues.getInt( 0 );
                }
                catch ( IOException | InvalidRangeException e )
                {
                    LOGGER.debug( "The reference time variable in '" +
                                  file.getLocation() +
                                  "' cannot be used to determine the "
                                  + "initialization time of the source data.",
                                  e );
                }

                if (reference != null)
                {
                    try
                    {
                        int lastIndex = time.getDimension(0).getLength() - 1;
                        Array validTimes = time.read(new int[]{lastIndex}, new int[]{1});
                        valid = validTimes.getInt( 0 );
                    }
                    catch ( IOException | InvalidRangeException e )
                    {
                        LOGGER.debug( "The time variable in '" +
                                      file.getLocation() +
                                      "' cannot be used to determine the valid "
                                      + "time of the source data.");
                    }
                }

                if (reference != null && valid != null)
                {
                    lead = valid - reference;
                    lead = ( int ) TimeHelper.unitsToLeadUnits( "MINUTES", lead );
                }
            }
        }

        if (lead == Integer.MAX_VALUE)
        {
            LOGGER.warn( "A proper lead time could not be determined for the "
                         + "forecast data in '{}'", file.getLocation() );
        }

        return lead;
    }

    public static String getInitializedTime( NetcdfFile file)
    {
        String initializationTime = null;

        Attribute initializationAttribute = file.findGlobalAttributeIgnoreCase("model_initialization_time");
        if (initializationAttribute != null)
        {
            initializationTime = initializationAttribute.getStringValue().replace("_", "T").trim();
        }

        return initializationTime;
    }

    public static String getValidTime(NetcdfFile file)
    {
        String validTime = null;
        Attribute validTimeAttribute = file.findGlobalAttributeIgnoreCase("model_output_valid_time");
        if (validTimeAttribute != null)
        {
            validTime = validTimeAttribute.getStringValue().replace("_", "T").trim();
        }
        return validTime;
    }

    public static Attribute getVariableAttribute(final Variable variable, final String attributeName)
    {
        return Collections.find(variable.getAttributes(), attribute ->
            attribute.getShortName().equalsIgnoreCase(attributeName)
        );
    }

    private static String[] getNWMFilenameParts(NetcdfFile file)
    {
        String name = Strings.getFileName(file.getLocation());
        name = Strings.removePattern(name, "\\.gz");
        name = Strings.removePattern(name, "nwm\\.");
        name = Strings.removePattern(name, "\\.nc");
        return name.split("\\.");
    }

    public static boolean isNetCDFFile(String filename)
    {
        return NetCDF.NETCDF_FILENAME_PATTERN.matcher( filename ).matches();
    }

    private static boolean isNWMData(NetcdfFile file)
    {
        return NetCDF.isNWMData( Strings.getFileName(file.getLocation()) );
    }

    private static boolean isNWMData(String filename)
    {
        return NetCDF.NWM_NAME_PATTERN.matcher( filename ).matches();
    }


    private static String getNWMCategory(NetcdfFile file)
    {
        return Strings.extractWord( Paths.get(file.getLocation()).getFileName().toString(),
                                    NWM_CATEGORY_PATTERN,
                                    "Unknown" );
    }

    public static Ensemble getEnsemble(NetcdfFile file)
    {
        String name = "Unknown";
        String tMinus = "0";

        if (NetCDF.isNWMData( file ))
        {
            String[] parts = NetCDF.getNWMFilenameParts( file );
            name = Collections.find( parts, possibility ->
                    Strings.hasValue( possibility ) &&
                    ( possibility.endsWith( "range" ) || possibility.endsWith(
                            "assim" ) ) );

            if (name.endsWith( "assim" ))
            {
                String minus = Collections.find(parts,
                                                possibility ->
                                                        Strings.hasValue( possibility ) &&
                                                        possibility.startsWith( "tm" )
                );

                if (Strings.hasValue( minus ))
                {
                    tMinus = Strings.extractWord( minus, "\\d\\d" );
                }
            }
        }

        Path location = Paths.get( file.getLocation());
        String qualifier = "";
        ArrayList<String> partList = new ArrayList<>(  );

        for (int part = 0; part < 3; ++part)
        {
            if (location.getParent() == null)
            {
                break;
            }

            location = location.getParent();

            // If this portion of the path isn't something like "nwm.20180411", we append it
            // We can't vouch for any other patterns, but anything containing
            // a pattern like that is bound to be hyper unique where it really shouldn't be
            if (!NetCDF.SHORT_DATE_PATTERN.matcher( location.getFileName().toString() ).matches())
            {
                partList.add( 0, location.getFileName().toString() );
            }
        }

        partList.add(NetCDF.getNWMCategory( file ));

        qualifier = String.join( ":", partList );

        return new Ensemble( name, qualifier, tMinus );
    }

    public static Integer getXLength(Variable var)
    {
        int length = 0;

        switch (var.getDimensions().size())
        {
            case 1:
            case 2:
                length = var.getDimension(0).getLength();
                break;
            case 3:
                length = var.getDimension(1).getLength();
                break;
        }

        return length;
    }

    public static Integer getYLength(Variable var)
    {
        Integer length = null;

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

    // TODO: We need a way to determine if the first variable is time and the
    // second variable is the coordinate; in that case, this will return true,
    // but the data will be vector, not gridded.
    public static boolean isGridded(Variable var)
    {
        Integer length = NetCDF.getYLength(var);

        return length != null && length > 0;
    }

    public static double getScaleFactor(Variable var)
    {
        double scaleFactor = 1.0;
        Attribute factor = NetCDF.getVariableAttribute(var, "scale_factor");

        if (factor != null)
        {
            scaleFactor = factor.getNumericValue().doubleValue();
        }

        return scaleFactor;
    }

    public static double getMinimumValue(Variable var)
    {
        double minimum = -Double.MAX_VALUE;

        Attribute range = NetCDF.getVariableAttribute( var, "valid_range" );

        if (range != null)
        {
            minimum = range.getNumericValue( 0 ).doubleValue();
        }

        return minimum;
    }

    public static double getMaximumValue(Variable var)
    {
        double maximum = Double.MAX_VALUE;

        Attribute range = NetCDF.getVariableAttribute( var, "valid_range" );

        if (range != null)
        {
            maximum = range.getNumericValue( 1 ).doubleValue();
        }

        return maximum;
    }

    public static double getAddOffset(Variable var)
    {
        double addOffset = 0.0;

        Attribute offset = NetCDF.getVariableAttribute(var, "add_offset");

        if (offset != null)
        {
            addOffset = offset.getNumericValue().doubleValue();
        }

        return addOffset;
    }

    public static Double getMissingValue(Variable var)
    {
        Double value = null;

        Attribute missingValue = NetCDF.getVariableAttribute(var, "missing_value");
        if (missingValue == null)
        {
            missingValue = NetCDF.getVariableAttribute(var, "_FillValue");
        }

        if (missingValue != null)
        {
            value = missingValue.getNumericValue().doubleValue() * NetCDF.getScaleFactor(var);
        }

        return value;
    }

    public static Double getGlobalMissingValue(NetcdfFile file)
    {
        Double value = null;

        Attribute missingValue = file.findGlobalAttribute("missing_value");

        if (missingValue == null)
        {
            missingValue = file.findGlobalAttribute("_FillValue");
        }

        if (missingValue != null)
        {
            value = missingValue.getNumericValue().doubleValue();
        }

        return value;
    }
}
