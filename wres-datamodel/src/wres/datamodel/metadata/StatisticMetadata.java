package wres.datamodel.metadata;

import java.util.Comparator;
import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.statistics.Statistic;

/**
 * An immutable store of metadata associated with a {@link Statistic}. Includes a {@link StatisticMetadataBuilder} for 
 * incremental construction.
 * 
 * @author james.brown@hydrosolved.com
 */
public class StatisticMetadata implements Comparable<StatisticMetadata>
{

    /**
     * The {@link SampleMetadata} associated with the {@link SampleData} from which the {@link Statistic} was computed.
     */

    private final SampleMetadata sampleMetadata;

    /**
     * The sample size.
     */

    private final int sampleSize;

    /**
     * The {@link MeasurementUnit} associated with the {@link Statistic}.
     */

    private final MeasurementUnit statisticUnit;

    /**
     * The metric identifier.
     */

    private final MetricConstants metricID;

    /**
     * The metric component identifier.
     */

    private final MetricConstants componentID;

    /**
     * Builds a {@link StatisticMetadata} with an input source and an override for the sample size.
     * 
     * @param source the input source
     * @param sampleSize the sample size
     * @return a {@link StatisticMetadata} object
     * @throws NullPointerException if the source is null
     */

    public static StatisticMetadata of( final StatisticMetadata source,
                                        final int sampleSize )
    {
        return new StatisticMetadataBuilder().setFromExistingInstance( source )
                                             .setSampleSize( sampleSize )
                                             .build();
    }

    /**
     * Builds a {@link StatisticMetadata} with an input source and an override for the metric identifiers.
     * 
     * @param source the input source
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @return a {@link StatisticMetadata} object
     * @throws NullPointerException if the source is null
     */

    public static StatisticMetadata of( final StatisticMetadata source,
                                        final MetricConstants metricID,
                                        final MetricConstants componentID )
    {
        return new StatisticMetadataBuilder().setFromExistingInstance( source )
                                             .setMetricID( metricID )
                                             .setMetricComponentID( componentID )
                                             .build();
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
     * @throws NullPointerException if the source is null
     */

    public static StatisticMetadata of( final SampleMetadata source,
                                        final MetricConstants metricID,
                                        final MetricConstants componentID,
                                        final boolean hasRealUnits,
                                        final int sampleSize,
                                        final DatasetIdentifier baselineID )
    {
        MeasurementUnit statisticUnit = null;

        //Dimensioned?
        if ( hasRealUnits )
        {
            statisticUnit = source.getMeasurementUnit();
        }
        else
        {
            statisticUnit = MeasurementUnit.of();
        }

        DatasetIdentifier identifier = source.getIdentifier();

        //Add the scenario ID associated with the baseline input
        if ( Objects.nonNull( baselineID ) )
        {
            identifier =
                    DatasetIdentifier.of( identifier, baselineID.getScenarioID() );
        }

        return StatisticMetadata.of( new SampleMetadataBuilder().setFromExistingInstance( source )
                                                                .setIdentifier( identifier )
                                                                .build(),
                                     sampleSize,
                                     statisticUnit,
                                     metricID,
                                     componentID );
    }

    /**
     * Returns an instance from the inputs.
     * 
     * @param source the sample metadata
     * @param sampleSize the sample size
     * @param statisticUnit the required output dimension
     * @param metricID the optional metric identifier
     * @param componentID the optional metric component identifier or decomposition template
     * @return a {@link StatisticMetadata} object
     * @throws NullPointerException if the source is null or the statisticUnit is null
     */

    public static StatisticMetadata of( final SampleMetadata source,
                                        final int sampleSize,
                                        final MeasurementUnit statisticUnit,
                                        final MetricConstants metricID,
                                        final MetricConstants componentID )
    {
        return new StatisticMetadataBuilder().setSampleMetadata( source )
                                             .setSampleSize( sampleSize )
                                             .setMeasurementUnit( statisticUnit )
                                             .setMetricID( metricID )
                                             .setMetricComponentID( componentID )
                                             .build();
    }

    /**
     * Returns the {@link SampleMetadata} that describes the {@link SampleData} from which the {@link Statistic} 
     * described by this {@link StatisticMetadata} was computed.
     * 
     * @return the sample metadata
     */

    public SampleMetadata getSampleMetadata()
    {
        return this.sampleMetadata;
    }

    /**
     * Returns <code>true</code> if the {@link #getMetricComponentID()} has been set, otherwise <code>false</code>.
     * 
     * @return true if the metric component identifier is defined, otherwise false
     */

    public boolean hasMetricComponentID()
    {
        return Objects.nonNull( this.getMetricComponentID() );
    }

    /**
     * Returns an identifier associated with the metric that produced the output.
     * 
     * @return the metric identifier
     */

    public MetricConstants getMetricID()
    {
        return this.metricID;
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
        return this.componentID;
    }

    /**
     * Returns the measurement unit associated with the {@link Statistic} which may differ from that associated with 
     * the {@link SampleData}. The {@link MeasurementUnit} for the {@link SampleData} is returned by 
     * {@link #getSampleMetadata()}.
     * 
     * @return the measurement unit
     */

    public MeasurementUnit getMeasurementUnit()
    {
        return this.statisticUnit;
    }

    /**
     * Returns the sample size associated with the {@link Statistic}.
     * 
     * @return the sample size
     */

    public int getSampleSize()
    {
        return this.sampleSize;
    }

    /**
     * <p>
     * Returns <code>true</code> if the input is minimally equal to this {@link StatisticMetadata}, otherwise
     * <code>false</code>. The two metadata objects are minimally equal if all of the following are equal, otherwise 
     * they are minimally unequal (and hence also unequal in terms of the stricter {@link Object#equals(Object)}.
     * </p>
     * <ol>
     * <li>{@link SampleMetadata#getMeasurementUnit()}</li>
     * <li>{@link #getMeasurementUnit()}</li>
     * <li>{@link #getMetricID()}</li>
     * <li>{@link #getMetricComponentID()}</li>
     * </ol>
     * 
     * @param meta the metadata to check
     * @return true if the mandatory elements match, false otherwise
     */

    public boolean minimumEquals( StatisticMetadata meta )
    {
        return meta.getMetricID() == this.getMetricID()
               && meta.getMetricComponentID() == this.getMetricComponentID()
               && meta.getMeasurementUnit().equals( this.getMeasurementUnit() )
               && meta.getSampleMetadata().getMeasurementUnit().equals( this.getSampleMetadata().getMeasurementUnit() );
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof StatisticMetadata ) )
        {
            return false;
        }
        final StatisticMetadata p = ( (StatisticMetadata) o );
        boolean returnMe = p.getSampleMetadata().equals( this.getSampleMetadata() )
                           && p.getSampleSize() == this.getSampleSize()
                           && p.getMeasurementUnit().equals( this.getMeasurementUnit() );
        return returnMe && p.getMetricID() == this.getMetricID()
               && p.getMetricComponentID() == this.getMetricComponentID();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getSampleMetadata(),
                             this.getSampleSize(),
                             this.getMetricID(),
                             this.getMetricComponentID(),
                             this.getMeasurementUnit() );
    }

    @Override
    public int compareTo( StatisticMetadata input )
    {
        Objects.requireNonNull( input, "Specify non-null metadata for comparison." );

        // Check the sample data metadata
        int returnMe = this.getSampleMetadata().compareTo( input.getSampleMetadata() );
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

        // Compare the measurement unit
        Comparator<MeasurementUnit> compareInputUnits = Comparator.nullsFirst( Comparator.naturalOrder() );
        return Objects.compare( this.getMeasurementUnit(),
                                input.getMeasurementUnit(),
                                compareInputUnits );
    }

    @Override
    public String toString()
    {
        String start = this.getSampleMetadata().toString();
        start = start.substring( 0, start.length() - 1 ); // Remove bookend char, ')'
        final StringBuilder b = new StringBuilder( start );
        b.append( "," )
         .append( this.getMeasurementUnit() )
         .append( "," )
         .append( this.getSampleSize() )
         .append( "," )
         .append( this.getMetricID() )
         .append( "," )
         .append( this.getMetricComponentID() )
         .append( ")" );
        return b.toString();
    }

    /**
     * Builder.
     */

    public static class StatisticMetadataBuilder
    {

        /**
         * The {@link SampleMetadata} associated with the {@link SampleData} from which the {@link Statistic} was computed.
         */

        private SampleMetadata sampleMetadata;

        /**
         * The sample size.
         */

        private int sampleSize;

        /**
         * The {@link MeasurementUnit} associated with the {@link Statistic}.
         */

        private MeasurementUnit statisticUnit;

        /**
         * The metric identifier.
         */

        private MetricConstants metricID;

        /**
         * The metric component identifier.
         */

        private MetricConstants componentID;

        /**
         * Sets the sample metadata.
         * 
         * @param sampleMetadata the sample metadata
         * @return the builder
         */

        public StatisticMetadataBuilder setSampleMetadata( SampleMetadata sampleMetadata )
        {
            this.sampleMetadata = sampleMetadata;
            return this;
        }

        /**
         * Sets the sample size.
         * 
         * @param sampleSize the sample size
         * @return the builder
         */

        public StatisticMetadataBuilder setSampleSize( int sampleSize )
        {
            this.sampleSize = sampleSize;
            return this;
        }

        /**
         * Sets the measurement unit.
         * 
         * @param statisticUnit the measurement unit
         * @return the builder
         */

        public StatisticMetadataBuilder setMeasurementUnit( MeasurementUnit statisticUnit )
        {
            this.statisticUnit = statisticUnit;
            return this;
        }

        /**
         * Sets the metric identifier.
         * 
         * @param metricID the metric identifier
         * @return the builder
         */

        public StatisticMetadataBuilder setMetricID( MetricConstants metricID )
        {
            this.metricID = metricID;
            return this;
        }

        /**
         * Sets the metric component identifier.
         * 
         * @param componentID the metric component identifier
         * @return the builder
         */

        public StatisticMetadataBuilder setMetricComponentID( MetricConstants componentID )
        {
            this.componentID = componentID;
            return this;
        }

        /**
         * Sets the contents from an existing metadata instance.
         * 
         * @param statisticMetadata the source metadata
         * @throws NullPointerException if the input is null
         * @return the builder
         */

        public StatisticMetadataBuilder setFromExistingInstance( StatisticMetadata statisticMetadata )
        {
            Objects.requireNonNull( statisticMetadata, "Specify non-null source metadata." );

            this.sampleMetadata = statisticMetadata.sampleMetadata;
            this.sampleSize = statisticMetadata.sampleSize;
            this.statisticUnit = statisticMetadata.statisticUnit;
            this.metricID = statisticMetadata.metricID;
            this.componentID = statisticMetadata.componentID;

            return this;
        }

        /**
         * Build the metadata.
         * 
         * @return the metadata instance
         */

        public StatisticMetadata build()
        {
            return new StatisticMetadata( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws NullPointerException if the sampleMetadata is null or the statisticUnit is null
     */

    private StatisticMetadata( StatisticMetadataBuilder builder )
    {
        // Set then validate
        this.sampleMetadata = builder.sampleMetadata;
        this.sampleSize = builder.sampleSize;
        this.statisticUnit = builder.statisticUnit;
        this.componentID = builder.componentID;
        this.metricID = builder.metricID;

        Objects.requireNonNull( this.sampleMetadata, "Specify non-null sample metadata." );

        Objects.requireNonNull( this.statisticUnit, "Specify a non-null measurement unit for the statistic" );

    }

}
