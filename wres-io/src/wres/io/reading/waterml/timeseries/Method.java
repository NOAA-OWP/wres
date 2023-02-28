package wres.io.reading.waterml.timeseries;

import java.io.Serial;
import java.io.Serializable;

/**
 * A method.
 */
public class Method implements Serializable
{
    @Serial
    private static final long serialVersionUID = 6026373155500052354L;

    /** Method description. */
    private String methodDescription;
    /** Method ID. */
    private Integer methodID;

    /**
     * @return the method description
     */
    public String getMethodDescription()
    {
        return methodDescription;
    }

    /**
     * Sets the method description.
     * @param methodDescription the method description
     */
    public void setMethodDescription( String methodDescription )
    {
        this.methodDescription = methodDescription;
    }

    /**
     * @return the method ID
     */
    public Integer getMethodID()
    {
        return methodID;
    }

    /**
     * Sets the method ID.
     * @param methodID the method ID
     */
    public void setMethodID( Integer methodID )
    {
        this.methodID = methodID;
    }
}
