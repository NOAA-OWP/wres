package wres.util;

import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Created by ctubbs on 7/7/17.
 */
public final class NetCDF {

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

    public static long getLeadTime(NetcdfFile file)
    {
        String initializationTime = getInitializedTime(file);
        String validTime = getValidTime(file);

        if (initializationTime == null || initializationTime.isEmpty())
        {
            throw new IllegalArgumentException("The passed in NetCDF File does not have a valid Initialization time.");
        }
        else if (validTime == null || validTime.isEmpty())
        {
            throw new IllegalArgumentException("The passed in NetCDF File does not have a valid valid time.");
        }

        OffsetDateTime initializedOn = Time.convertStringToDate(initializationTime);
        OffsetDateTime validOn = Time.convertStringToDate(validTime);

        return initializedOn.until(validOn, ChronoUnit.HOURS);
    }

    public static String getInitializedTime(NetcdfFile file)
    {
        String initializationTime = null;

        Attribute initializationAttribute = getGlobalAttribute(file, "model_initialization_time");
        if (initializationAttribute != null)
        {
            initializationTime = initializationAttribute.getStringValue().replace("_", " ").trim();
        }

        return initializationTime;
    }

    public static String getValidTime(NetcdfFile file)
    {
        String validTime = null;
        Attribute validTimeAttribute = getGlobalAttribute(file, "model_output_valid_time");
        if (validTimeAttribute != null)
        {
            validTime = validTimeAttribute.getStringValue().replace("_", " ").trim();
        }
        return validTime;
    }

    public static Attribute getGlobalAttribute(NetcdfFile file, String attributeName)
    {
        return Collections.find(file.getGlobalAttributes(), (Attribute attribute) -> {
            return attribute.getShortName().equalsIgnoreCase(attributeName);
        });
    }

    public static  Attribute getVariableAttribute(Variable variable, String attributeName)
    {
        return Collections.find(variable.getAttributes(), (Attribute attribute) -> {
            return attribute.getShortName().equalsIgnoreCase(attributeName);
        });
    }
}
