package wres.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Created by ctubbs on 7/7/17.
 */
public final class NetCDF {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetCDF.class);
    private static final String NWM_NAME_PATTERN =
            "^nwm\\.t\\d\\dz\\.(short|medium|long|analysis)_(range|assim)\\.[a-zA-Z]+(_rt)?(_\\d)?\\.(f\\d\\d\\d|tm\\d\\d)\\.conus\\.nc(\\.gz)?$";;

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
        boolean variableExists = false;

        for (int variableIndex = 0; variableIndex < file.getVariables().size(); ++variableIndex)
        {
            if (file.getVariables().get(variableIndex).getShortName().equalsIgnoreCase(variableName))
            {
                variableExists = true;
                break;
            }
        }

        return variableExists;
    }

    /**
     * Attempts to retrieve the variable with the given short name from within the passed in NetCDF file
     * @param file The file to search through
     * @param variableName The short name of the variable to retrieve
     * @return If the variable exists, the variable is retrieved. Otherwise, it is null.
     */
    public static Variable getVariable(NetcdfFile file, String variableName)
    {
        Variable foundVariable = null;

        for (int variableIndex = 0; variableIndex < file.getVariables().size(); ++variableIndex)
        {
            if (file.getVariables().get(variableIndex).getShortName().equalsIgnoreCase(variableName))
            {
                foundVariable = file.getVariables().get(variableIndex);
                break;
            }
        }

        return foundVariable;
    }


    public static Integer getNWMLeadTime(NetcdfFile file)
    {
        if (!NetCDF.isNWMData(file))
        {
            throw new IllegalArgumentException("The NetCDF data in '" +
                                                       file.getLocation() +
                                                       "' is not valid National Water Model Data.");
        }

        Integer lead = null;
        String leadDescription;

        OffsetDateTime initialization = Time.convertStringToDate(NetCDF.getInitializedTime(file));
        OffsetDateTime valid = Time.convertStringToDate(NetCDF.getValidTime(file));

        return ((Long)initialization.until(valid, ChronoUnit.HOURS)).intValue();
    }

    public static String getNWMRange(NetcdfFile file)
    {
        if (!NetCDF.isNWMData(file))
        {
            throw new IllegalArgumentException("The NetCDF data in '" +
                                                       file.getLocation() +
                                                       "' is not valid National Water Model Data.");
        }
        String range = Collections.find(NetCDF.getNWMFilenameParts(file), (String possibility) -> {
            return Strings.hasValue(possibility) && (possibility.endsWith("range") || possibility.endsWith("assim"));
        });

        // This is somewhat like hitting it with a hammer. Without this line, we get results like "short_range" or
        // "analysis_assim", both of which aren't completely human friendy ('_' = bad), nor are they in the forecast
        // type table
        range = Strings.removePattern(range, "_[a-zA-Z]+");
        return range;
    }

    public static String getInitializedTime(NetcdfFile file)
    {
        String initializationTime = null;

        Attribute initializationAttribute = file.findGlobalAttributeIgnoreCase("model_initialization_time");
        if (initializationAttribute != null)
        {
            initializationTime = initializationAttribute.getStringValue().replace("_", " ").trim();
        }

        return initializationTime;
    }

    public static String getValidTime(NetcdfFile file)
    {
        String validTime = null;
        Attribute validTimeAttribute = file.findGlobalAttributeIgnoreCase("model_output_valid_time");
        if (validTimeAttribute != null)
        {
            validTime = validTimeAttribute.getStringValue().replace("_", " ").trim();
        }
        return validTime;
    }

    public static Attribute getVariableAttribute(Variable variable, String attributeName)
    {
        return Collections.find(variable.getAttributes(), (Attribute attribute) -> {
            return attribute.getShortName().equalsIgnoreCase(attributeName);
        });
    }

    public static String[] getNWMFilenameParts(NetcdfFile file)
    {
        String name = Strings.getFileName(file.getLocation());
        name = Strings.removePattern(name, "\\.gz");
        name = Strings.removePattern(name, "nwm\\.");
        name = Strings.removePattern(name, "\\.nc");
        return name.split("\\.");
    }

    public static boolean isNWMData(NetcdfFile file)
    {
        boolean matches = Strings.getFileName(file.getLocation()).matches(NetCDF.NWM_NAME_PATTERN);

        if (!matches)
        {
            LOGGER.warn("The NetCDF file at '{}' does not conform to National Water Model Standards.",
                        file.getLocation());
        }

        return matches;
    }

    public static String getNWMCategory(NetcdfFile file)
    {
        if (!NetCDF.isNWMData(file))
        {
            throw new IllegalArgumentException("The NetCDF file at '" +
                                                       file.getLocation() +
                                                       "' is not valid National Water Model Data.");
        }

        return Strings.extractWord(Strings.getFileName(file.getLocation()),
                                   "(?<=(assim|range)\\.)[a-zA-Z_\\d]+(?=\\.(tm\\d\\d|f\\d\\d\\d))");
    }

    public static String getEnsemble(NetcdfFile file)
    {
        String ensembleName = "Default";

        String range = NetCDF.getNWMRange(file);

        if (Strings.hasValue(range) && range.equalsIgnoreCase("long_range"))
        {
            String category = NetCDF.getNWMCategory(file);
            ensembleName = Strings.extractWord(category, "(?<=_)\\d");
        }

        return ensembleName;
    }

    public static Integer getXLength(Variable var)
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
