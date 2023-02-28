package wres.io.reading.waterml.timeseries;

import java.io.Serial;
import java.io.Serializable;

/**
 * A side code.
 */
public class SiteCode implements Serializable
{
    @Serial
    private static final long serialVersionUID = -7293845985686518859L;

    /** Value. */
    private String value;
    /** Network. */
    private String network;
    /** Agency code. */
    private String agencyCode;

    /**
     * @return the value
     */
    public String getValue()
    {
        return value;
    }

    /**
     * Sets the value.
     * @param value the value
     */
    public void setValue( String value )
    {
        this.value = value;
    }

    /**
     * @return the network
     */
    public String getNetwork()
    {
        return network;
    }

    /**
     * Sets the network
     * @param network tje network
     */
    public void setNetwork( String network )
    {
        this.network = network;
    }

    /**
     * @return the agency code
     */
    public String getAgencyCode()
    {
        return agencyCode;
    }

    /**
     * Sets the agency code.
     * @param agencyCode the agency code
     */
    public void setAgencyCode( String agencyCode )
    {
        this.agencyCode = agencyCode;
    }
}
