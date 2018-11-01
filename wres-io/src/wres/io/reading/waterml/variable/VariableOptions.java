package wres.io.reading.waterml.variable;

import java.io.Serializable;

public class VariableOptions implements Serializable
{
    public Option[] getOption()
    {
        return option;
    }

    public void setOption( Option[] option )
    {
        this.option = option;
    }

    Option[] option;
}
