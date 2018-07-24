package wres.datamodel.metadata;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.outputs.MetricOutput;

/**
 * An immutable store of metadata associated with a {@link MetricOutput}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MetricOutputMetadata extends Metadata
{
    /**
     * The sample size.
     */

    private final int sampleSize;

    /**
     * The dimension associated with the input data from which the output was computed.
     */

    private final Dimension inputDim;

    /**
     * The metric identifier.
     */

    private final MetricConstants metricID;

    /**
     * The metric component identifier.
     */

    private final MetricConstants componentID;

    /**
     * Returns an instance from the inputs.
     * 
     * @param sampleSize the sample size
     * @param outputDim the required output dimension
     * @param inputDim the optional input dimension
     * @param metricID the optional metric identifier
     * @param componentID the optional metric component identifier or decomposition template
     * @param identifier the optional dataset identifier
     * @param timeWindow the optional time window
     * @return a {@link MetricOutputMetadata} object
     * @throws NullPointerException if the output dimension is null
     */

    public static MetricOutputMetadata of( int sampleSize,
                                           Dimension outputDim,
                                           Dimension inputDim,
                                           MetricConstants metricID,
                                           MetricConstants componentID,
                                           DatasetIdentifier identifier,
                                           TimeWindow timeWindow )
    {
        return new MetricOutputMetadata( sampleSize,
                                         outputDim,
                                         inputDim,
                                         metricID,
                                         componentID,
                                         identifier,
                                         timeWindow );
    }

    /**
     * Returns <code>true</code> if the {@link #getMetricComponentID()} has been set, otherwise <code>false</code>.
     * 
     * @return true if the metric component identifier is defined, otherwise false
     */

    public boolean hasMetricComponentID()
    {
        return Objects.nonNull( getMetricComponentID() );
    }

    /**
     * Returns an identifier associated with the metric that produced the output.
     * 
     * @return the metric identifier
     */

    public MetricConstants getMetricID()
    {
        return metricID;
    }

    /**
     * Returns an optional identifier associated with the component of the metric to which the output corresponds or 
     * a template for a score decomposition where the output contains multiple components. In that case, the template 
     * dictates the order in which components are returned.
     * 
     * @return the component identifier or null
     */

    public MetricConstants getMetricComponentID()
    {
        return componentID;
    }

    /**
     * Returns the dimension associated with the metric input, which may differ from the output. The output dimension
     * is returned by {@link #getDimension()}.
     * 
     * @return the dimension
     */

    public Dimension getInputDimension()
    {
        return inputDim;
    }

    /**
     * Returns the sample size associated with the {@link MetricOutput}.
     * 
     * @return the sample size
     */

    public int getSampleSize()
    {
        return sampleSize;
    }

    /**
     * <p>
     * Returns <code>true</code> if the input is minimally equal to this {@link MetricOutputMetadata}, otherwise
     * <code>false</code>. The two metadata objects are minimally equal if all of the following are equal, otherwise 
     * they are minimally unequal (and hence also unequal in terms of the stricter {@link Object#equals(Object)}.
     * </p>
     * <ol>
     * <li>{@link #getDimension()}</li>
     * <li>{@link #getInputDimension()}</li>
     * <li>{@link #getMetricID()}</li>
     * <li>{@link #getMetricComponentID()}</li>
     * </ol>
     * 
     * @param meta the metadata to check
     * @return true if the mandatory elements match, false otherwise
     */

    public boolean minimumEquals( MetricOutputMetadata meta )
    {
        return meta.getMetricID() == getMetricID()
               && meta.getMetricComponentID() == getMetricComponentID()
               && meta.getDimension().equals( getDimension() )
               && meta.getInputDimension().equals( getInputDimension() );
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof MetricOutputMetadata ) )
        {
            return false;
        }
        final MetricOutputMetadata p = ( (MetricOutputMetadata) o );
        boolean returnMe = super.equals( o ) && p.getSampleSize() == getSampleSize()
                           && p.getInputDimension().equals( getInputDimension() );
        return returnMe && p.getMetricID() == getMetricID()
               && p.getMetricComponentID() == getMetricComponentID();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(),
                             getSampleSize(),
                             getMetricID(),
                             getMetricComponentID(),
                             getInputDimension() );
    }

    @Override
    public String toString()
    {
        String start = super.toString();
        start = start.substring( 0, start.length() - 1 ); // Remove bookend char, ']'
        final StringBuilder b = new StringBuilder( start );
        b.append( "," )
         .append( inputDim )
         .append( "," )
         .append( sampleSize )
         .append( "," )
         .append( metricID )
         .append( "," )
         .append( componentID )
         .append( "]" );
        return b.toString();
    }

    /**
     * Hidden constructor.
     * 
     * @param sampleSize the sample size
     * @param outputDim the required output dimension
     * @param inputDim the optional input dimension
     * @param metricID the optional metric identifier
     * @param componentID the optional metric component identifier or decomposition template
     * @param identifier the optional dataset identifier
     * @param timeWindow the optional time window
     * @return a {@link MetricOutputMetadata} object
     * @throws NullPointerException if the output dimension is null
     */

    private MetricOutputMetadata( int sampleSize,
                                  Dimension outputDim,
                                  Dimension inputDim,
                                  MetricConstants metricID,
                                  MetricConstants componentID,
                                  DatasetIdentifier identifier,
                                  TimeWindow timeWindow )
    {
        super( outputDim, identifier, timeWindow );

        this.sampleSize = sampleSize;
        this.inputDim = inputDim;
        this.componentID = componentID;
        this.metricID = metricID;
    }

}
