package wres.util;

import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

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
}
