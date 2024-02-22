package wres.reading.waterml.variable;

import java.io.Serial;
import java.io.Serializable;

/**
 * A unit.
 */
public class Unit implements Serializable
{
    @Serial
    private static final long serialVersionUID = 3361311118306617838L;
    /** Unit code. */
    private String unitCode;

    /**
     * @return the unit code
     */
    public String getUnitCode()
    {
        return unitCode;
    }

    /**
     * Sets the unit code.
     * @param unitCode the unit code
     */
    public void setUnitCode( String unitCode )
    {
        this.unitCode = unitCode;
    }
}
