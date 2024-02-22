package wres.reading.waterml.variable;

import java.io.Serializable;

/**
 * An option.
 */
public class Option implements Serializable
{
    private static final long serialVersionUID = 142625806918589450L;
    /** Name. */
    private String name;
    /** Option code. */
    private String optionCode;

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Sets the name.
     * @param name the name
     */
    public void setName( String name )
    {
        this.name = name;
    }

    /**
     * @return the option code
     */
    public String getOptionCode()
    {
        return optionCode;
    }

    /**
     * Sets the option code.
     * @param optionCode the option code
     */
    public void setOptionCode( String optionCode )
    {
        this.optionCode = optionCode;
    }
}
