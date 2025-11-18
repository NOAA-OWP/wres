package wres.reading.nwis.iv.response.timeseries;

import java.io.Serial;
import java.io.Serializable;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * A method.
 * @param methodDescription the method description
 * @param methodID the method ID
 */
public record Method( String methodDescription,
                      Integer methodID ) implements Serializable
{
    @Serial
    private static final long serialVersionUID = 6026373155500052354L;

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "methodID", this.methodID() )
                .append( "methodDescription",
                         this.methodDescription )
                .build();
    }
}
