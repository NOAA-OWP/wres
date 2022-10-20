package wres.io.reading.waterml.variable;

import java.io.Serializable;

public class VariableOptions implements Serializable
{
    private static final long serialVersionUID = -3592017658139874856L;
    
    Option[] option;
    
    public Option[] getOption()
    {
        return option;
    }

    public void setOption( Option[] option )
    {
        this.option = option;
    }
}
