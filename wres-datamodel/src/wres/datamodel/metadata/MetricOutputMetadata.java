package wres.datamodel.metadata;

import java.util.Comparator;
import java.util.Objects;

import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.statistics.MetricOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;

/**
 * An immutable store of metadata associated with a {@link MetricOutput}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MetricOutputMetadata extends Metadata implements Comparable<MetricOutputMetadata>
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
                                        source.getThresholds(),
                                        source.getProjectConfig() );
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
                                        thresholds,
                                        source.getProjectConfig() );
    }

    /**
     * Builds a {@link MetricOutputMetadata} with an input source and an override for the metric identifiers.
     * 
     * @param source the input source
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata of( final MetricOutputMetadata source,
                                           final MetricConstants metricID,
                                           final MetricConstants componentID )
    {
        return MetricOutputMetadata.of( source.getSampleSize(),
                                        source.getMeasurementUnit(),
                                        source.getInputDimension(),
                                        metricID,
                                        componentID,
                                        source.getIdentifier(),
                                        source.getTimeWindow(),
                                        source.getThresholds(),
                                        source.getProjectConfig() );
    }
    
    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed source of {@link Metadata} whose parameters are
     * copied, together with a sample size, a {@link MeasurementUnit} for the output, and {@link MetricConstants} 
     * identifiers for the metric and the metric component, respectively.
     * 
     * @param source the source metadata
     * @param sampleSize the sample size
     * @param outputDim the output dimension
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @return a {@link MetricOutputMetadata} object
     * @throws NullPointerException if the input metadata is null
     */

    public static MetricOutputMetadata of( final Metadata source,
                                           final int sampleSize,
                                           final MeasurementUnit outputDim,
                                           final MetricConstants metricID,
                                           final MetricConstants componentID )
    {
        Objects.requireNonNull( source,
                                "Specify a non-null source of input metadata from which to build the output metadata." );

        return MetricOutputMetadata.of( sampleSize,
                                        outputDim,
                                        source.getMeasurementUnit(),
                                        metricID,
                                        componentID,
                                        source.getIdentifier(),
                                        source.getTimeWindow(),
                                        source.getThresholds(),
                                        source.getProjectConfig() );
    }    

    /**
     * Returns an instance from the inputs.
     * 
     * @param source the source metadata
     * @param metricID the metric identifier
     * @param componentID the component identifier or metric decomposition template
     * @param hasRealUnits is true if the metric produces outputs with real units, false for dimensionless units
     * @param sampleSize the sample size
     * @param baselineID the baseline identifier or null
     * @return the output metadata
     */

    public static MetricOutputMetadata of( final Metadata source,
                                           final MetricConstants metricID,
                                           final MetricConstants componentID,
                                           final boolean hasRealUnits,
                                           final int sampleSize,
                                           final DatasetIdentifier baselineID )
    {
        MeasurementUnit outputDim = null;

        //Dimensioned?
        if ( hasRealUnits )
        {
            outputDim = source.getMeasurementUnit();
        }
        else
        {
            outputDim = MeasurementUnit.of();
        }

        DatasetIdentifier identifier = source.getIdentifier();

        //Add the scenario ID associated with the baseline input
        if ( Objects.nonNull( baselineID ) )
        {
            identifier =
                    DatasetIdentifier.of( identifier, baselineID.getScenarioID() );
        }

        return MetricOutputMetadata.of( sampleSize,
                   outputDim,
                   source.getMeasurementUnit(),
                   metricID,
                   componentID,
                   identifier,
                   source.getTimeWindow(),
                   source.getThresholds(),
                   source.getProjectConfig() );
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
                                        null,
                                        null );
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
                                        null,
                                        null );
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
     * @param projectConfig the optional project configuration
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
                                           OneOrTwoThresholds thresholds,
                                           ProjectConfig projectConfig )
    {
        return new MetricOutputMetadata( sampleSize,
                                         outputDim,
                                         inputDim,
                                         metricID,
                                         componentID,
                                         identifier,
                                         timeWindow,
                                         thresholds,
                                         projectConfig );
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
    public int compareTo( MetricOutputMetadata input )
    {
        Objects.requireNonNull( input, "Specify non-null metadata for comparison." );

        // Check measurement units, which are always available
        int returnMe = this.getMeasurementUnit().compareTo( input.getMeasurementUnit() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Check identifier via the string representation
        returnMe = Objects.compare( this.getIdentifier() + "", input.getIdentifier() + "", Comparator.naturalOrder() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Check the time window
        Comparator<TimeWindow> compareWindows = Comparator.nullsFirst( Comparator.naturalOrder() );
        returnMe = Objects.compare( this.getTimeWindow(), input.getTimeWindow(), compareWindows );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Check the thresholds
        Comparator<OneOrTwoThresholds> compareThresholds = Comparator.nullsFirst( Comparator.naturalOrder() );
        returnMe = Objects.compare( this.getThresholds(), input.getThresholds(), compareThresholds );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Compare sample sizes
        returnMe = Integer.compare( this.getSampleSize(), input.getSampleSize() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Compare metric identifiers
        Comparator<MetricConstants> compareMetrics = Comparator.nullsFirst( Comparator.naturalOrder() );
        returnMe = Objects.compare( this.getMetricID(), input.getMetricID(), compareMetrics );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Compare metric component identifiers
        Comparator<MetricConstants> compareMetricComps = Comparator.nullsFirst( Comparator.naturalOrder() );
        returnMe = Objects.compare( this.getMetricComponentID(), input.getMetricComponentID(), compareMetricComps );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Compare the input dimension
        Comparator<MeasurementUnit> compareInputUnits = Comparator.nullsFirst( Comparator.naturalOrder() );
        return Objects.compare( this.getInputDimension(), input.getInputDimension(), compareInputUnits );
    }

    @Override
    public String toString()
    {
        String start = super.toString();
        start = start.substring( 0, start.length() - 1 ); // Remove bookend char, ')'
        final StringBuilder b = new StringBuilder( start );
        b.append( "," )
         .append( inputDim )
         .append( "," )
         .append( sampleSize )
         .append( "," )
         .append( metricID )
         .append( "," )
         .append( componentID )
         .append( ")" );
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
     * @param projectConfig the optional project configuration
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
                                  OneOrTwoThresholds thresholds,
                                  ProjectConfig projectConfig )
    {
        super( outputDim, identifier, timeWindow, thresholds, projectConfig );

        this.sampleSize = sampleSize;
        this.inputDim = inputDim;
        this.componentID = componentID;
        this.metricID = metricID;
    }

}
