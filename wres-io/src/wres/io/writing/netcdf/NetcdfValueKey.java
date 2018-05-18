package wres.io.writing.netcdf;

class NetcdfValueKey
{
    private final String variableName;
    private final int[] origin;
    private final double value;

    NetcdfValueKey(final String variableName, final int[] origin, final double value)
    {
        this.variableName = variableName;
        this.origin = origin;
        this.value = value;
    }

    double getValue()
    {
        return this.value;
    }

    int[] getOrigin()
    {
        return this.origin;
    }

    String getVariableName()
    {
        return this.variableName;
    }
}
