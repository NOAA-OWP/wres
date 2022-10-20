package wres.io.reading.waterml.variable;

import java.io.Serializable;

public class Option implements Serializable
{
    private static final long serialVersionUID = 142625806918589450L;
    
    String name;

    public String getName()
    {
        return name;
    }

    public void setName( String name )
    {
        this.name = name;
    }

    public String getOptionCode()
    {
        return optionCode;
    }

    public void setOptionCode( String optionCode )
    {
        this.optionCode = optionCode;
    }

    String optionCode;
}
