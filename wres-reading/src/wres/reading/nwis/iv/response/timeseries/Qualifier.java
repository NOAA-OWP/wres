package wres.reading.nwis.iv.response.timeseries;

import java.io.Serial;
import java.io.Serializable;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * A qualifier.
 * @param qualifierCode the qualifier code
 * @param qualifierDescription the qualifier description
 * @param qualifierID the qualifier ID
 * @param network the network
 * @param vocabulary the vocabulary
 */
public record Qualifier( String qualifierCode,
                         String qualifierDescription,
                         Integer qualifierID,
                         String network,
                         String vocabulary ) implements Serializable
{
    @Serial
    private static final long serialVersionUID = -3380625359503743133L;

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "qualifierID", this.qualifierID() )
                .append( "qualifierCode", this.qualifierCode() )
                .append( "qualifierDescription", this.qualifierDescription() )
                .append( "network", this.network() )
                .append( "vocabulary", this.vocabulary() )
                .build();
    }
}
