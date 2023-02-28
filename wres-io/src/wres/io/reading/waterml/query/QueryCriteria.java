package wres.io.reading.waterml.query;

import java.io.Serial;
import java.io.Serializable;

/**
 * The query criteria.
 */

public class QueryCriteria implements Serializable
{
    @Serial
    private static final long serialVersionUID = -6329750451321834556L;

    /** Location parameter. */
    private String locationParam;
    /** Variable parameter. */
    private String variableParam;
    /** Time parameter. */
    private QueryTimeParameter timeParam;
    /** Parameter. */
    private String[] parameter;

    /**
     * @return the location parameter
     */
    public String getLocationParam()
    {
        return locationParam;
    }

    /**
     * Sets the location parameter.
     * @param locationParam the location parameter
     */
    public void setLocationParam( String locationParam )
    {
        this.locationParam = locationParam;
    }

    /**
     * @return the variable parameter
     */
    public String getVariableParam()
    {
        return variableParam;
    }

    /**
     * Sets the variable parameter.
     * @param variableParam the variable parameter
     */
    public void setVariableParam( String variableParam )
    {
        this.variableParam = variableParam;
    }

    /**
     * @return the time parameter
     */
    public QueryTimeParameter getTimeParam()
    {
        return timeParam;
    }

    /**
     * Sets the time parameter.
     * @param timeParam the time parameter
     */
    public void setTimeParam( QueryTimeParameter timeParam )
    {
        this.timeParam = timeParam;
    }

    /**
     * @return the parameters
     */
    public String[] getParameter()
    {
        return parameter;
    }

    /**
     * Sets the parameters.
     * @param parameter the parameters
     */
    public void setParameter( String[] parameter )
    {
        this.parameter = parameter;
    }
}
