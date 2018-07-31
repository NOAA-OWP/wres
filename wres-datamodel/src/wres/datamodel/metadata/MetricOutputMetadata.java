package wres.datamodel.metadata;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;

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

    private final MeasurementUnit inputDim;

    /**
     * The metric identifier.
     */

    private final MetricConstants metricID;

    /**
     * The metric component identifier.
     */

    private final MetricConstants componentID;

    /**
     * Builds a {@link MetricOutputMetadata} with an input source and an override for the sample size.
     * 
     * @param source the input source
     * @param sampleSize the sample size
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata of( final MetricOutputMetadata source,
                                           final int sampleSize )
    {
        return MetricOutputMetadata.of( sampleSize,
                                        source.getMeasurementUnit(),
                                        source.getInputDimension(),
                                        source.getMetricID(),
                                        source.getMetricComponentID(),
                                        source.getIdentifier(),
                                        source.getTimeWindow(),
                                        source.getThresholds() );
    }

    /**
     * Builds a {@link MetricOutputMetadata} with an input source and an override for the time window and thresholds.
     * 
     * @param source the input source
     * @param timeWindow the optional time window
     * @param thresholds the optional thresholds
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata of( final MetricOutputMetadata source,
                                           final TimeWindow timeWindow,
                                           final OneOrTwoThresholds thresholds )
    {
        return MetricOutputMetadata.of( source.getSampleSize(),
                                        source.getMeasurementUnit(),
                                        source.getInputDimension(),
                                        source.getMetricID(),
                                        source.getMetricComponentID(),
                                        source.getIdentifier(),
                                        timeWindow,
                                        thresholds );
    }

    /**
     * Builds a {@link MetricOutputMetadata} with an input source and an override for the metric component identifier.
     * 
     * @param source the input source
     * @param componentID the metric component identifier or decomposition template
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata of( final MetricOutputMetadata source,
                                           final MetricConstants componentID )
    {
        return MetricOutputMetadata.of( source.getSampleSize(),
                                        source.getMeasurementUnit(),
                                        source.getInputDimension(),
                                        source.getMetricID(),
                                        componentID,
                                        source.getIdentifier(),
                                        source.getTimeWindow(),
                                        source.getThresholds() );
    }

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, a {@link MeasurementUnit} for the output
     * and the input, and a {@link MetricConstants} identifier for the metric.
     * 
     * @param sampleSize the sample size
     * @param outputDim the dimension
     * @param inputDim the input dimension
     * @param metricID the metric identifier
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata of( final int sampleSize,
                                           final MeasurementUnit outputDim,
                                           final MeasurementUnit inputDim,
                                           final MetricConstants metricID )
    {
        return MetricOutputMetadata.of( sampleSize,
                                        outputDim,
                                        inputDim,
                                        metricID,
                                        MetricConstants.MAIN,
                                        null,
                                        null,
                                        null );
    }

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, a {@link MeasurementUnit} for the output
     * and the input, and {@link MetricConstants} identifiers for the metric and the metric component, respectively.
     * 
     * @param sampleSize the sample size
     * @param outputDim the output dimension
     * @param inputDim the input dimension
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata of( final int sampleSize,
                                           final MeasurementUnit outputDim,
                                           final MeasurementUnit inputDim,
                                           final MetricConstants metricID,
                                           final MetricConstants componentID )
    {
        return MetricOutputMetadata.of( sampleSize,
                                        outputDim,
                                        inputDim,
                                        metricID,
                                        componentID,
                                        null,
                                        null,
                                        null );
    }

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed source of {@link Metadata} whose parameters are
     * copied, together with a sample size, a {@link MeasurementUnit} for the output, and {@link MetricConstants} identifiers
     * for the metric and the metric component, respectively.
     * 
     * @param sampleSize the sample size
     * @param outputDim the output dimension
     * @param metadata the source metadata
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @return a {@link MetricOutputMetadata} object
     * @throws NullPointerException if the input metadata is null
     */

    public static MetricOutputMetadata of( final int sampleSize,
                                           final MeasurementUnit outputDim,
                                           final Metadata metadata,
                                           final MetricConstants metricID,
                                           final MetricConstants componentID )
    {
        Objects.requireNonNull( metadata,
                                "Specify a non-null source of input metadata from which to build the output metadata." );

        return MetricOutputMetadata.of( sampleSize,
                                        outputDim,
                                        metadata.getMeasurementUnit(),
                                        metricID,
                                        componentID,
                                        metadata.getIdentifier(),
                                        metadata.getTimeWindow(),
                                        metadata.getThresholds() );
    }

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, a {@link MeasurementUnit} for the output
     * and the input, {@link MetricConstants} identifiers for the metric and the metric component, respectively, and an
     * optional {@link DatasetIdentifier} identifier.
     * 
     * @param sampleSize the sample size
     * @param outputDim the output dimension
     * @param inputDim the input dimension
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @param identifier an optional dataset identifier (may be null)
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata of( final int sampleSize,
                                           final MeasurementUnit outputDim,
                                           final MeasurementUnit inputDim,
                                           final MetricConstants metricID,
                                           final MetricConstants componentID,
                                           final DatasetIdentifier identifier )
    {
        return MetricOutputMetadata.of( sampleSize,
                                        outputDim,
                                        inputDim,
                                        metricID,
                                        componentID,
                                        identifier,
                                        null,
                                        null );
    }
    

    /**
     * Returns an instance from the inputs.
     * 
     * @param metIn the metric input metadata
     * @param metricId the metric identifier
     * @param componentId the component identifier or metric decomposition template
     * @param hasRealUnits is true if the metric produces outputs with real units, false for dimensionless units
     * @param sampleSize the sample size
     * @param baselineID the baseline identifier or null
     * @return the output metadata
     */

    public static MetricOutputMetadata of( final Metadata metIn,
                                           final MetricConstants metricId,
                                           final MetricConstants componentId,
                                           final boolean hasRealUnits,
                                           final int sampleSize,
                                           final DatasetIdentifier baselineID )
    {
        MeasurementUnit outputDim = null;

        //Dimensioned?
        if ( hasRealUnits )
        {
            outputDim = metIn.getMeasurementUnit();
        }
        else
        {
            outputDim = MeasurementUnit.of();
        }

        DatasetIdentifier identifier = metIn.getIdentifier();

        //Add the scenario ID associated with the baseline input
        if ( Objects.nonNull( baselineID ) )
        {
            identifier =
                    DatasetIdentifier.of( identifier, baselineID.getScenarioID() );
        }

        return of( sampleSize,
                   outputDim,
                   metIn.getMeasurementUnit(),
                   metricId,
                   componentId,
                   identifier,
                   metIn.getTimeWindow(),
                   metIn.getThresholds() );
    }

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
     * @param thresholds the optional thresholds
     * @return a {@link MetricOutputMetadata} object
     * @throws NullPointerException if the output dimension is null
     */

    public static MetricOutputMetadata of( int sampleSize,
                                           MeasurementUnit outputDim,
                                           MeasurementUnit inputDim,
                                           MetricConstants metricID,
                                           MetricConstants componentID,
                                           DatasetIdentifier identifier,
                                           TimeWindow timeWindow,
                                           OneOrTwoThresholds thresholds )
    {
        return new MetricOutputMetadata( sampleSize,
                                         outputDim,
                                         inputDim,
                                         metricID,
                                         componentID,
                                         identifier,
                                         timeWindow,
                                         thresholds );
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
     * is returned by {@link #getMeasurementUnit()}.
     * 
     * @return the dimension
     */

    public MeasurementUnit getInputDimension()
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
     * <li>{@link #getMeasurementUnit()}</li>
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
               && meta.getMeasurementUnit().equals( getMeasurementUnit() )
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
     * @param thresholds the optional thresholds
     * @return a {@link MetricOutputMetadata} object
     * @throws NullPointerException if the output dimension is null
     */

    private MetricOutputMetadata( int sampleSize,
                                  MeasurementUnit outputDim,
                                  MeasurementUnit inputDim,
                                  MetricConstants metricID,
                                  MetricConstants componentID,
                                  DatasetIdentifier identifier,
                                  TimeWindow timeWindow,
                                  OneOrTwoThresholds thresholds )
    {
        super( outputDim, identifier, timeWindow, thresholds );

        this.sampleSize = sampleSize;
        this.inputDim = inputDim;
        this.componentID = componentID;
        this.metricID = metricID;
    }

}
