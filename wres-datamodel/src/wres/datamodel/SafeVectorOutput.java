package wres.datamodel;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.VectorOutput;

/**
 * An immutable vector of outputs associated with a metric. The number of outputs, as well as the individual outputs and
 * the order in which they are stored, is prescribed by the metric from which the outputs originate.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeVectorOutput implements VectorOutput
{

    /**
     * The output.
     */

    private final VectorOfDoubles output;

    /**
     * The metadata associated with the output.
     */

    private final MetricOutputMetadata meta;

    /**
     * The template associated with the outputs.
     */

    private ScoreOutputGroup template;

    @Override
    public MetricOutputMetadata getMetadata()
    {
        return meta;
    }

    @Override
    public VectorOfDoubles getData()
    {
        return output;
    }

    @Override
    public ScoreOutputGroup getOutputTemplate()
    {
        return template;
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof SafeVectorOutput ) )
        {
            return false;
        }
        final SafeVectorOutput v = (SafeVectorOutput) o;
        return meta.equals( v.getMetadata() ) && template.equals( v.template )
               && Arrays.equals( output.getDoubles(), v.getData().getDoubles() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( meta, template, output );
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( ",", "[", "]" );
        for ( double next : output.getDoubles() )
        {
            joiner.add( Double.toString( next ) );
        }
        return joiner.toString();
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param template the output template
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeVectorOutput( final VectorOfDoubles output,
                      final ScoreOutputGroup template,
                      final MetricOutputMetadata meta )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricOutputException( "Specify a non-null output." );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( "Specify non-null metadata." );
        }
        if ( Objects.isNull( template ) )
        {
            throw new MetricOutputException( "Specify a non-null output group for the vector output." );
        }
        //Check that the decomposition template is compatible
        if ( template.getMetricComponents().size() != output.size() )
        {
            throw new MetricOutputException( "The specified output template '" + template
                                             + "' has more components than metric inputs provided ["
                                             + template.getMetricComponents().size()
                                             + ", "
                                             + output.size()
                                             + "]." );
        }
        this.output = ( (DefaultDataFactory) DefaultDataFactory.getInstance() ).safeVectorOf( output );
        this.meta = meta;
        this.template = template;
    }

    @Override
    public double getValue( MetricConstants component )
    {
        return output.getDoubles()[template.getMetricComponents().indexOf( component )];
    }

}
