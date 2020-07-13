package wres.datamodel.sampledata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.Pool;
import wres.statistics.generated.ValueFilter;

/**
 * An immutable store of metadata associated with {@link SampleData}. Includes a {@link Builder} for 
 * incremental construction.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SampleMetadata implements Comparable<SampleMetadata>
{

    private static final Logger LOGGER = LoggerFactory.getLogger( SampleMetadata.class );

    /**
     * A description of the evaluation.
     */

    private final Evaluation evaluation;

    /**
     * A description of the pool to which the sample data belongs.
     */

    private final Pool pool;

    /**
     * TODO: eliminate this glue. The {@link location} is only needed because it maps inadequately to a canonical 
     * {@link Geometry}. In order for wrappers like {@link SampleMetadata} to work when canonical forms are sent on the 
     * wire they must wrap canonical forms 1:1 - they cannot wrap local abstractions, such as {@link Location} or 
     * whatever replaces {@link Location} after #72747, otherwise the canonical information sent on the wire would not 
     * be sufficient to build a {@link SampleMetadata}. Changes are needed in two steps. In the first step, eliminate 
     * this glue and include a {@link Geometry} within a {@link DatasetIdentifier}, which used on construction of a 
     * {@link SampleMetadata}. In the second step, do not allow for construction of a {@link SampleMetadata} from 
     * non-canonical forms, such as {@link DatasetIdentifier}, rather from an {@link Evaluation} plus a {@link Pool}, 
     * nothing else.
     */
    @Deprecated
    private final Location location;

    /**
     * Build a {@link SampleMetadata} object with a default {@link MeasurementUnit}.
     * 
     * @return a {@link SampleMetadata} object
     */

    public static SampleMetadata of()
    {
        return new Builder().setMeasurementUnit( MeasurementUnit.of() ).build();
    }

    /**
     * Build a {@link SampleMetadata} object with a sample size and a prescribed {@link MeasurementUnit}.
     * 
     * @param unit the required measurement unit
     * @return a {@link SampleMetadata} object
     */

    public static SampleMetadata of( MeasurementUnit unit )
    {
        return new Builder().setMeasurementUnit( unit ).build();
    }

    /**
     * Build a {@link SampleMetadata} object with a prescribed {@link MeasurementUnit} and an optional 
     * {@link DatasetIdentifier}.
     * 
     * @param unit the required measurement unit
     * @param identifier an optional dataset identifier (may be null)
     * @return a {@link SampleMetadata} object
     */

    public static SampleMetadata of( MeasurementUnit unit, DatasetIdentifier identifier )
    {
        return new Builder().setMeasurementUnit( unit ).setIdentifier( identifier ).build();
    }

    /**
     * Returns an instance from the inputs.
     * 
     * @param unit the required measurement unit
     * @param identifier an optional dataset identifier
     * @param timeWindow an optional time window
     * @param thresholds an optional set of thresholds
     * @throws NullPointerException if the dimension is null
     * @return a metadata instance
     */

    public static SampleMetadata of( MeasurementUnit unit,
                                     DatasetIdentifier identifier,
                                     TimeWindowOuter timeWindow,
                                     OneOrTwoThresholds thresholds )
    {
        return new Builder().setMeasurementUnit( unit )
                            .setIdentifier( identifier )
                            .setTimeWindow( timeWindow )
                            .setThresholds( thresholds )
                            .build();
    }

    /**
     * Builds a {@link SampleMetadata} from a prescribed input source and an override {@link OneOrTwoThresholds}.
     * 
     * @param input the source metadata
     * @param thresholds the thresholds
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static SampleMetadata of( SampleMetadata input, OneOrTwoThresholds thresholds )
    {
        return new Builder( input ).setThresholds( thresholds ).build();
    }

    /**
     * Builds a {@link SampleMetadata} from a prescribed input source and an override {@link TimeWindowOuter}.
     * 
     * @param input the source metadata
     * @param timeWindow the new time window
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static SampleMetadata of( SampleMetadata input, TimeWindowOuter timeWindow )
    {
        return new Builder( input ).setTimeWindow( timeWindow ).build();
    }

    /**
     * Builds a {@link SampleMetadata} from a prescribed input source and an override {@link TimeScaleOuter}.
     * 
     * @param input the source metadata
     * @param timeScale the new time scale
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static SampleMetadata of( SampleMetadata input, TimeScaleOuter timeScale )
    {
        return new Builder( input ).setTimeScale( timeScale ).build();
    }

    /**
     * Builds a {@link SampleMetadata} from a prescribed input source and an override {@link TimeWindowOuter} and 
     * {@link TimeScaleOuter}.
     * 
     * @param input the source metadata
     * @param timeWindow the new time window
     * @param timeScale the new time scale
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static SampleMetadata of( SampleMetadata input,
                                     TimeWindowOuter timeWindow,
                                     TimeScaleOuter timeScale )
    {
        return new Builder( input ).setTimeWindow( timeWindow )
                                   .setTimeScale( timeScale )
                                   .build();
    }

    /**
     * Builds a {@link SampleMetadata} from a prescribed input source and an override {@link TimeWindowOuter} and 
     * {@link OneOrTwoThresholds}.
     * 
     * @param input the source metadata
     * @param timeWindow the new time window
     * @param thresholds the thresholds
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static SampleMetadata of( SampleMetadata input,
                                     TimeWindowOuter timeWindow,
                                     OneOrTwoThresholds thresholds )
    {
        return new Builder( input ).setThresholds( thresholds )
                                   .setTimeWindow( timeWindow )
                                   .build();
    }

    /**
     * Finds the union of the input, based on the {@link TimeWindowOuter}. All components of the input must be equal, 
     * except the {@link SampleMetadata#getTimeWindow()} and {@link SampleMetadata#getThresholds()}, otherwise an 
     * exception is thrown. See also {@link TimeWindowOuter#unionOf(List)}. No threshold information is represented in the 
     * union.
     * 
     * @param input the input metadata
     * @return the union of the input
     * @throws IllegalArgumentException if the input is empty
     * @throws NullPointerException if the input is null
     * @throws SampleMetadataException if the input contains metadata whose differences extend beyond the time windows and
     *            thresholds
     */

    public static SampleMetadata unionOf( List<SampleMetadata> input )
    {
        String nullString = "Cannot find the union of null metadata.";

        Objects.requireNonNull( input, nullString );

        if ( input.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot find the union of empty input." );
        }
        List<TimeWindowOuter> unionWindow = new ArrayList<>();

        // Test entry
        SampleMetadata test = input.get( 0 );

        // Validate for equivalence with the first entry and add window to list
        for ( SampleMetadata next : input )
        {
            Objects.requireNonNull( next, nullString );

            if ( !next.equalsWithoutTimeWindowOrThresholds( test ) )
            {
                throw new SampleMetadataException( "Only the time window and thresholds can differ when finding the "
                                                   + "union of metadata." );
            }
            if ( next.hasTimeWindow() )
            {
                unionWindow.add( next.getTimeWindow() );
            }
        }

        // Remove any threshold information from the result
        test = SampleMetadata.of( test, (OneOrTwoThresholds) null );

        if ( !unionWindow.isEmpty() )
        {
            test = SampleMetadata.of( test, TimeWindowOuter.unionOf( unionWindow ) );
        }
        return test;
    }

    @Override
    public int compareTo( SampleMetadata input )
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
        Comparator<TimeWindowOuter> compareWindows = Comparator.nullsFirst( Comparator.naturalOrder() );
        returnMe = Objects.compare( this.getTimeWindow(), input.getTimeWindow(), compareWindows );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Check the thresholds
        Comparator<OneOrTwoThresholds> compareThresholds = Comparator.nullsFirst( Comparator.naturalOrder() );
        return Objects.compare( this.getThresholds(), input.getThresholds(), compareThresholds );
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof SampleMetadata ) )
        {
            return false;
        }
        SampleMetadata p = (SampleMetadata) o;
        return Objects.equals( this.getEvaluation(), p.getEvaluation() )
               && Objects.equals( this.getPool(), p.getPool() )
               && Objects.equals( this.location, p.location );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getEvaluation(),
                             this.getPool(),
                             this.location );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                            .append( "datasetIdentifier",
                                                                                     this.getIdentifier() )
                                                                            .append( "timeWindow",
                                                                                     this.getTimeWindow() )
                                                                            .append( "thresholds",
                                                                                     this.getThresholds() )
                                                                            .append( "timeScale",
                                                                                     this.getTimeScale() )
                                                                            .append( "measurementUnit",
                                                                                     this.getMeasurementUnit() )
                                                                            .build();
    }

    /**
     * Returns <code>true</code> if {@link #getIdentifier()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getIdentifier()} returns non-null, false otherwise.
     */
    public boolean hasIdentifier()
    {
        return Objects.nonNull( this.getIdentifier() );
    }

    /**
     * Returns <code>true</code> if {@link #getTimeWindow()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getTimeWindow()} returns non-null, false otherwise.
     */
    public boolean hasTimeWindow()
    {
        return Objects.nonNull( this.getTimeWindow() );
    }

    /**
     * Returns <code>true</code> if {@link #getThresholds()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getThresholds()} returns non-null, false otherwise.
     */
    public boolean hasThresholds()
    {
        return Objects.nonNull( this.getThresholds() );
    }

    /**
     * Returns <code>true</code> if {@link #getTimeScale()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getTimeScale()} returns non-null, false otherwise.
     */
    public boolean hasTimeScale()
    {
        return Objects.nonNull( this.getTimeScale() );
    }

    /**
     * Returns <code>true</code> if the input is equal to the current {@link SampleMetadata} without considering the 
     * {@link #getTimeWindow()} or {@link #getThresholds()}.
     * 
     * @param input the input metadata
     * @return true if the input is equal to the current metadata, without considering the time window or thresholds
     */
    public boolean equalsWithoutTimeWindowOrThresholds( final SampleMetadata input )
    {
        if ( Objects.isNull( input ) )
        {
            return false;
        }
        boolean returnMe =
                input.getMeasurementUnit().equals( this.getMeasurementUnit() )
                           && this.hasIdentifier() == input.hasIdentifier()
                           && this.hasTimeScale() == input.hasTimeScale();

        // The following tests apply where both the existing and input attributes are non-null,
        // as equivalent null status is tested above
        if ( this.hasIdentifier() )
        {
            returnMe = returnMe && this.getIdentifier().equals( input.getIdentifier() );
        }

        if ( this.hasTimeScale() )
        {
            returnMe = returnMe && this.getTimeScale().equals( input.getTimeScale() );
        }

        return returnMe;
    }

    /**
     * Returns the measurement unit associated with the metric.
     * 
     * @return the measurement unit
     */

    public MeasurementUnit getMeasurementUnit()
    {
        String unit = this.getEvaluation().getMeasurementUnit();
        return MeasurementUnit.of( unit );
    }

    /**
     * Returns an optional dataset identifier or null.
     * 
     * @return an identifier or null
     */

    public DatasetIdentifier getIdentifier()
    {
        DatasetIdentifier returnMe = null;

        Location localLocation = this.location;
        LeftOrRightOrBaseline localContext = LeftOrRightOrBaseline.RIGHT;
        String variableName = null;
        String scenario = null;
        String baselineScenario = null;

        if ( !this.getEvaluation().getVariableName().isBlank() )
        {
            variableName = this.getEvaluation().getVariableName();
        }

        if ( !this.getEvaluation().getRightSourceName().isBlank() )
        {
            scenario = this.getEvaluation().getRightSourceName();
        }

        if ( !this.getEvaluation().getBaselineSourceName().isBlank() )
        {
            baselineScenario = this.getEvaluation().getBaselineSourceName();
        }

        if ( this.getPool().getIsBaselinePool() )
        {
            localContext = LeftOrRightOrBaseline.BASELINE;
        }

        if ( Objects.nonNull( localLocation ) || Objects.nonNull( variableName )
             || Objects.nonNull( scenario )
             || Objects.nonNull( baselineScenario ) )
        {
            returnMe = DatasetIdentifier.of( localLocation, variableName, scenario, baselineScenario, localContext );
        }

        return returnMe;
    }

    /**
     * Returns a {@link TimeWindowOuter} associated with the metadata or null.
     * 
     * @return a time window or null
     */

    public TimeWindowOuter getTimeWindow()
    {
        TimeWindowOuter outer = null;

        if ( this.getPool().hasTimeWindow() )
        {
            wres.statistics.generated.TimeWindow window = this.getPool().getTimeWindow();
            outer = new TimeWindowOuter.Builder( window ).build();
        }

        return outer;
    }

    /**
     * Returns a {@link OneOrTwoThresholds} associated with the metadata or null.
     * 
     * @return a set of thresholds or null
     */

    public OneOrTwoThresholds getThresholds()
    {
        OneOrTwoThresholds thresholds = null;

        if ( this.getPool().hasEventThreshold() )
        {
            wres.statistics.generated.Threshold event = this.getPool().getEventThreshold();
            ThresholdOuter eventOuter = new ThresholdOuter.Builder( event ).build();
            ThresholdOuter decisionOuter = null;

            if ( this.getPool().hasDecisionThreshold() )
            {
                wres.statistics.generated.Threshold decision = this.getPool().getDecisionThreshold();
                decisionOuter = new ThresholdOuter.Builder( decision ).build();
            }

            thresholds = OneOrTwoThresholds.of( eventOuter, decisionOuter );
        }

        return thresholds;
    }

    /**
     * Returns a {@link TimeScaleOuter} associated with the metadata or null.
     * 
     * @return the time scale or null
     */

    public TimeScaleOuter getTimeScale()
    {
        TimeScaleOuter outer = null;
        if ( this.getEvaluation().hasTimeScale() )
        {
            outer = TimeScaleOuter.of( this.getEvaluation().getTimeScale() );
        }

        return outer;
    }

    /**
     * Returns the evaluation description.
     * 
     * @return the evaluation description.
     */

    public Evaluation getEvaluation()
    {
        return this.evaluation;
    }

    /**
     * Returns the pool description.
     * 
     * @return the pool description.
     */

    public Pool getPool()
    {
        return this.pool;
    }

    /**
     * Builder. TODO: remove this builder and allow for construction from canonical forms only, specifically an
     * {@link Evaluation} and a {@link Pool}, nothing more, and both non-null.
     */

    @Deprecated
    public static class Builder
    {

        /**
         * Error message for null input.
         */

        private static final String NULL_INPUT_ERROR = "Specify a non-null source from which to build the metadata.";

        /**
         * The measurement unit associated with the data.
         */

        private MeasurementUnit unit;

        /**
         * An optional dataset identifier, may be null.
         */

        private DatasetIdentifier identifier;

        /**
         * An optional time window associated with the data, may be null.
         */

        private TimeWindowOuter timeWindow;

        /**
         * An optional set of thresholds associated with the data, may be null.
         */

        private OneOrTwoThresholds thresholds;

        /**
         * The optional {@link ProjectConfig} associated with the metadata, may be null.
         */

        private ProjectConfig projectConfig;

        /**
         * The optional time scale information.
         */

        private TimeScaleOuter timeScale;

        /**
         * An existing evaluation description.
         */

        private Evaluation evaluation;

        /**
         * An existing pool description.
         */

        private Pool pool;

        /**
         * TODO: eliminate this glue. Replace the {@link Location} inside the {@link DatasetIdentifier} with a canonical 
         * {@link Geometry}. It is currently needed because there is a poor mapping between a {@link Location} and
         * a canonical {@link Geometry}. Eventually, remove all options to build instances of this class from 
         * non-canonical forms, such as the {@link DatasetIdentifier}. The only reason that has not been done upfront is 
         * to stage the refactoring.
         */

        private Location location;

        /**
         * Sets the measurement unit.
         * 
         * @param unit the measurement unit
         * @return the builder
         */

        public Builder setMeasurementUnit( MeasurementUnit unit )
        {
            this.unit = unit;
            return this;
        }

        /**
         * Sets the identifier.
         * 
         * @param identifier the identifier
         * @return the builder
         */

        public Builder setIdentifier( DatasetIdentifier identifier )
        {
            this.identifier = identifier;
            return this;
        }

        /**
         * Sets the time window.
         * 
         * @param timeWindow the time window
         * @return the builder
         */

        public Builder setTimeWindow( TimeWindowOuter timeWindow )
        {
            this.timeWindow = timeWindow;
            return this;
        }

        /**
         * Sets the thresholds.
         * 
         * @param thresholds the thresholds
         * @return the builder
         */

        public Builder setThresholds( OneOrTwoThresholds thresholds )
        {
            this.thresholds = thresholds;
            return this;
        }

        /**
         * Sets the project declaration.
         * 
         * @param projectConfig the project declaration
         * @return the builder
         */

        public Builder setProjectConfig( ProjectConfig projectConfig )
        {
            this.projectConfig = projectConfig;
            return this;
        }

        /**
         * Sets the time scale information.
         * 
         * @param timeScale the time scale
         * @return the builder
         */

        public Builder setTimeScale( TimeScaleOuter timeScale )
        {
            this.timeScale = timeScale;
            return this;
        }

        /**
         * Build the metadata.
         * 
         * @return the metadata instance
         */

        public SampleMetadata build()
        {
            return new SampleMetadata( this );
        }

        /**
         * No argument constructor.
         */

        public Builder()
        {
        }

        /**
         * Construct with existing evaluation and pool instances.
         * 
         * @param evaluation the evaluation
         * @param pool the pool
         */

        public Builder( Evaluation evaluation, Pool pool )
        {
            this.evaluation = evaluation;
            this.pool = pool;
        }

        /**
         * Sets the contents from an existing metadata instance.
         * 
         * @param sampleMetadata the source metadata
         * @throws NullPointerException if the input is null
         */

        public Builder( SampleMetadata sampleMetadata )
        {
            Objects.requireNonNull( sampleMetadata, NULL_INPUT_ERROR );

            this.evaluation = sampleMetadata.evaluation;
            this.pool = sampleMetadata.pool;
            this.location = sampleMetadata.location;
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws NullPointerException if the measurement unit has not been set
     */

    private SampleMetadata( Builder builder )
    {
        // Set then validate
        MeasurementUnit unit = builder.unit;
        DatasetIdentifier identifier = builder.identifier;
        TimeWindowOuter timeWindow = builder.timeWindow;
        OneOrTwoThresholds thresholds = builder.thresholds;
        ProjectConfig localProjectConfig = builder.projectConfig;
        TimeScaleOuter timeScale = builder.timeScale;
        Evaluation localEvaluation = builder.evaluation;
        Pool localPool = builder.pool;

        Evaluation.Builder evaluationBuilder = null;
        Pool.Builder poolBuilder = null;
        Location localLocation = builder.location;

        if ( Objects.isNull( localEvaluation ) )
        {
            evaluationBuilder = Evaluation.newBuilder();
        }
        else
        {
            evaluationBuilder = localEvaluation.toBuilder();
        }

        if ( Objects.isNull( localPool ) )
        {
            poolBuilder = Pool.newBuilder();
        }
        else
        {
            poolBuilder = localPool.toBuilder();
        }

        // TODO: replace the mapping between a Location and a Geometry
        // For now, this is deferred by storing a location instance locally
        if ( Objects.nonNull( identifier ) && identifier.hasLocation() )
        {
            localLocation = identifier.getLocation();

            LOGGER.debug( "While creating sample metadata, populated the evaluation with a location of {}.",
                          localLocation );
        }

        this.evaluation = this.getEvaluation( evaluationBuilder, unit, identifier, timeScale, localProjectConfig );
        this.pool = this.getPool( poolBuilder, timeWindow, thresholds );
        this.location = localLocation;

        this.validate();
    }

    /**
     * Sets the evaluation fields using the input and returns an instance of an evaluation.
     * 
     * @param evaluationBuilder the evaluation builder
     * @param unit the measurement units
     * @param identifier the dataset identifier
     * @param timeScale the time scale
     * @param projectConfig the project declaration
     * @return the evaluation
     */

    private Evaluation getEvaluation( wres.statistics.generated.Evaluation.Builder evaluationBuilder,
                                      MeasurementUnit unit,
                                      DatasetIdentifier identifier,
                                      TimeScaleOuter timeScale,
                                      ProjectConfig projectConfig )
    {

        // Populate the evaluation from the supplied information as reasonably as possible
        if ( Objects.nonNull( unit ) )
        {
            evaluationBuilder.setMeasurementUnit( unit.getUnit() );

            LOGGER.debug( "While creating sample metadata, populated the evaluation with a measurement unit of {}.",
                          unit.getUnit() );
        }

        if ( Objects.nonNull( identifier ) && identifier.hasVariableName() )
        {
            evaluationBuilder.setVariableName( identifier.getVariableName() );

            LOGGER.debug( "While creating sample metadata, populated the evaluation with a variable name of {}.",
                          identifier.getVariableName() );
        }

        if ( Objects.nonNull( identifier ) && identifier.hasScenarioName() )
        {
            evaluationBuilder.setRightSourceName( identifier.getScenarioName() );

            LOGGER.debug( "While creating sample metadata, populated the evaluation with a right source name of {}.",
                          identifier.getScenarioName() );
        }

        if ( Objects.nonNull( identifier ) && identifier.hasScenarioNameForBaseline() )
        {
            evaluationBuilder.setBaselineSourceName( identifier.getScenarioNameForBaseline() );

            LOGGER.debug( "While creating sample metadata, populated the evaluation with a baseline source name of "
                          + "{}.",
                          identifier.getScenarioNameForBaseline() );
        }

        if ( Objects.nonNull( timeScale ) )
        {
            wres.statistics.generated.TimeScale scale = MessageFactory.parse( timeScale );
            evaluationBuilder.setTimeScale( scale );

            LOGGER.debug( "While creating sample metadata, populated the evaluation with a time scale of "
                          + "{}.",
                          timeScale );
        }

        if ( Objects.nonNull( projectConfig ) && Objects.nonNull( projectConfig.getPair() )
             && Objects.nonNull( projectConfig.getPair().getSeason() ) )
        {
            wres.statistics.generated.Season season = MessageFactory.parse( projectConfig.getPair().getSeason() );
            evaluationBuilder.setSeason( season );

            LOGGER.debug( "While creating sample metadata, populated the evaluation with a season of "
                          + "{}.",
                          projectConfig.getPair().getSeason() );
        }

        if ( Objects.nonNull( projectConfig ) && Objects.nonNull( projectConfig.getPair() )
             && Objects.nonNull( projectConfig.getPair().getValues() ) )
        {
            ValueFilter filter = MessageFactory.parse( projectConfig.getPair().getValues() );
            evaluationBuilder.setValueFilter( filter );

            LOGGER.debug( "While creating sample metadata, populated the evaluation with a value filter of "
                          + "{}.",
                          projectConfig.getPair().getValues() );
        }

        if ( Objects.nonNull( projectConfig ) && Objects.nonNull( projectConfig.getMetrics() ) )
        {
            int metricCount = projectConfig.getMetrics()
                                           .stream()
                                           .mapToInt( a -> a.getMetric().size() )
                                           .sum();

            evaluationBuilder.setMetricMessageCount( metricCount );

            LOGGER.debug( "While creating sample metadata, populated the evaluation with a metric count of "
                          + "{}.",
                          metricCount );
        }

        return evaluationBuilder.build();
    }


    /**
     * Sets the pool fields using the input and returns an instance of a pool.
     * 
     * @param poolBuilder the pool builder
     * @param timeWindow the time window
     * @param thresholds the thresholds
     * @return the pool
     */

    private Pool getPool( wres.statistics.generated.Pool.Builder poolBuilder,
                          TimeWindowOuter timeWindow,
                          OneOrTwoThresholds thresholds )
    {

        // Populate the pool from the supplied information as reasonably as possible
        if ( Objects.nonNull( timeWindow ) )
        {
            wres.statistics.generated.TimeWindow window = MessageFactory.parse( timeWindow );
            poolBuilder.setTimeWindow( window );

            LOGGER.debug( "While creating pool metadata, populated the pool with a time window of {}.",
                          timeWindow );
        }

        // Populate the pool from the supplied information as reasonably as possible
        if ( Objects.nonNull( thresholds ) )
        {
            wres.statistics.generated.Threshold event = MessageFactory.parse( thresholds.first() );
            poolBuilder.setEventThreshold( event );

            if ( thresholds.hasTwo() )
            {
                wres.statistics.generated.Threshold decision = MessageFactory.parse( thresholds.second() );
                poolBuilder.setDecisionThreshold( decision );
            }

            LOGGER.debug( "While creating pool metadata, populated the pool with a threshold of {}.",
                          thresholds );
        }

        return poolBuilder.build();
    }

    /**
     * Validate the input.
     */

    private void validate()
    {
        String unit = this.getEvaluation().getMeasurementUnit();

        if ( unit.isBlank() )
        {
            throw new IllegalArgumentException( "Specify a valid measurement unit from which to build the metadata." );
        }
    }


}
