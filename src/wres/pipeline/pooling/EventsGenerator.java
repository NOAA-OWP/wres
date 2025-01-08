package wres.pipeline.pooling;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.CovariateDataset;
import wres.config.yaml.components.CovariatePurpose;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EventDetection;
import wres.config.yaml.components.EventDetectionCombination;
import wres.config.yaml.components.EventDetectionDataset;
import wres.config.yaml.components.TimeWindowAggregation;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.RescaledTimeSeriesPlusValidation;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowSlicer;
import wres.eventdetection.EventDetectionException;
import wres.eventdetection.EventDetector;
import wres.io.project.Project;
import wres.io.retrieving.RetrieverFactory;
import wres.statistics.generated.TimeScale;

/**
 * Generates {@link TimeWindowOuter} corresponding to events from {@link TimeSeries}.
 *
 * @param leftUpscaler the upscaler for single-valued time-series with a left orientation
 * @param rightUpscaler the upscaler for single-valued time-series with a right orientation
 * @param baselineUpscaler the upscaler for single-valued time-series with a baseline orientation
 * @param covariateUpscaler the upscaler for single-valued time-series with a covariate orientation
 * @param measurementUnit the measurement unit
 * @param eventDetector the event detector
 * @author James Brown
 */
record EventsGenerator( TimeSeriesUpscaler<Double> leftUpscaler,
                        TimeSeriesUpscaler<Double> rightUpscaler,
                        TimeSeriesUpscaler<Double> baselineUpscaler,
                        TimeSeriesUpscaler<Double> covariateUpscaler,
                        String measurementUnit,
                        EventDetector eventDetector )
{
    /** Repeated message. */
    private static final String DETECTED_EVENTS_IN_THE_DATASET = "Detected {} events in the {} dataset for feature "
                                                                 + "group {}.";

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( EventsGenerator.class );

    /**
     * Construct and validate.
     *
     * @param leftUpscaler the upscaler for single-valued time-series with a left orientation
     * @param rightUpscaler the upscaler for single-valued time-series with a right orientation
     * @param baselineUpscaler the upscaler for single-valued time-series with a baseline orientation
     * @param covariateUpscaler the upscaler for single-valued time-series with a covariate orientation
     * @param measurementUnit the measurement unit
     * @param eventDetector the event detector
     */
    EventsGenerator
    {
        Objects.requireNonNull( leftUpscaler );
        Objects.requireNonNull( rightUpscaler );
        Objects.requireNonNull( baselineUpscaler );
        Objects.requireNonNull( covariateUpscaler );
        Objects.requireNonNull( measurementUnit );
        Objects.requireNonNull( eventDetector );
    }

    /**
     * Performs event detection for one or more declared time-series datasets.
     * @param project the project whose time-series data should be used
     * @param featureGroup the feature group to use for event detection
     * @param eventRetriever the retriever for time-series data
     * @return the detected events
     */
    Set<TimeWindowOuter> doEventDetection( Project project,
                                           FeatureGroup featureGroup,
                                           RetrieverFactory<Double, Double, Double> eventRetriever )
    {
        LOGGER.info( "Performing event detection for feature group {}...", featureGroup.getName() );

        EvaluationDeclaration declaration = project.getDeclaration();
        EventDetection detection = declaration.eventDetection();
        EventDetectionCombination combination = detection.parameters()
                                                         .combination();
        Set<TimeWindowOuter> events = new HashSet<>();
        TimeScaleOuter desiredTimeScale = project.getDesiredTimeScale();

        boolean detectionAttempted = false; // Only attempt to combine events when detection has been attempted
        for ( EventDetectionDataset dataset : detection.datasets() )
        {
            switch ( dataset )
            {
                case COVARIATES ->
                {
                    List<CovariateDataset> filtered = declaration.covariates()
                                                                 .stream()
                                                                 .filter( n -> n.purposes()
                                                                                .contains( CovariatePurpose.DETECT ) )
                                                                 .toList();
                    LOGGER.debug( "Performing event detection for {} covariate datasets.", filtered.size() );

                    for ( CovariateDataset next : filtered )
                    {
                        Set<TimeWindowOuter> innerEvents = Set.of();

                        // Get the desired timescale appropriate for the covariate dataset
                        TimeScaleOuter covariateTimeScale =
                                this.getAdjustedTimeScale( desiredTimeScale, next.rescaleFunction() );

                        switch ( next.featureNameOrientation() )
                        {
                            case LEFT ->
                            {
                                EventDetectionDetails details =
                                        new EventDetectionDetails( EventDetectionDataset.COVARIATES,
                                                                   featureGroup,
                                                                   FeatureTuple::getLeft,
                                                                   eventRetriever,
                                                                   detection,
                                                                   next.dataset()
                                                                       .variable()
                                                                       .name(),
                                                                   covariateTimeScale,
                                                                   this.covariateUpscaler(),
                                                                   null );

                                innerEvents = this.doEventDetection( details );
                            }
                            case RIGHT ->
                            {
                                EventDetectionDetails details =
                                        new EventDetectionDetails( EventDetectionDataset.COVARIATES,
                                                                   featureGroup,
                                                                   FeatureTuple::getRight,
                                                                   eventRetriever,
                                                                   detection,
                                                                   next.dataset()
                                                                       .variable()
                                                                       .name(),
                                                                   covariateTimeScale,
                                                                   this.covariateUpscaler(),
                                                                   null );
                                innerEvents = this.doEventDetection( details );
                            }
                            case BASELINE ->
                            {
                                EventDetectionDetails details =
                                        new EventDetectionDetails( EventDetectionDataset.COVARIATES,
                                                                   featureGroup,
                                                                   FeatureTuple::getBaseline,
                                                                   eventRetriever,
                                                                   detection,
                                                                   next.dataset()
                                                                       .variable()
                                                                       .name(),
                                                                   covariateTimeScale,
                                                                   this.covariateUpscaler(),
                                                                   null );
                                innerEvents = this.doEventDetection( details );
                            }
                            case COVARIATE -> throw new IllegalStateException( "Covariate dataset cannot have a "
                                                                               + "covariate feature name "
                                                                               + "orientation." );
                        }

                        this.combineEvents( detectionAttempted, events, innerEvents, combination );
                        detectionAttempted = true;
                    }
                }
                case OBSERVED ->
                {
                    LOGGER.debug( "Performing event detection for an observed dataset." );
                    EventDetectionDetails details =
                            new EventDetectionDetails( dataset,
                                                       featureGroup,
                                                       FeatureTuple::getLeft,
                                                       eventRetriever,
                                                       detection,
                                                       null,
                                                       desiredTimeScale,
                                                       this.leftUpscaler(),
                                                       this.measurementUnit() );
                    Set<TimeWindowOuter> innerEvents = this.doEventDetection( details );
                    this.combineEvents( detectionAttempted, events, innerEvents, combination );
                    detectionAttempted = true;
                }
                case PREDICTED ->
                {
                    LOGGER.debug( "Performing event detection for a predicted dataset." );
                    EventDetectionDetails details =
                            new EventDetectionDetails( dataset,
                                                       featureGroup,
                                                       FeatureTuple::getRight,
                                                       eventRetriever,
                                                       detection,
                                                       null,
                                                       desiredTimeScale,
                                                       this.rightUpscaler(),
                                                       this.measurementUnit() );
                    Set<TimeWindowOuter> innerEvents = this.doEventDetection( details );
                    this.combineEvents( detectionAttempted, events, innerEvents, combination );
                    detectionAttempted = true;
                }
                case BASELINE ->
                {
                    LOGGER.debug( "Performing event detection for a baseline dataset." );
                    EventDetectionDetails details =
                            new EventDetectionDetails( dataset,
                                                       featureGroup,
                                                       FeatureTuple::getBaseline,
                                                       eventRetriever,
                                                       detection,
                                                       null,
                                                       desiredTimeScale,
                                                       this.baselineUpscaler(),
                                                       this.measurementUnit() );
                    Set<TimeWindowOuter> innerEvents = this.doEventDetection( details );
                    this.combineEvents( detectionAttempted, events, innerEvents, combination );
                    detectionAttempted = true;
                }
            }
        }

        LOGGER.info( "Detected {} events across all datasets for feature group {} when forming the {}.",
                     events.size(),
                     featureGroup.getName(),
                     combination );

        return this.aggregateEvents( events,
                                     detection.parameters()
                                              .aggregation(),
                                     featureGroup );
    }

    /**
     * Performs event detection for an {@link EventDetectionDataset} that is either
     * {@link EventDetectionDataset#OBSERVED}, {@link EventDetectionDataset#PREDICTED} or
     * {@link EventDetectionDataset#BASELINE}.
     *
     * @param details the event detection details
     * @throws NullPointerException if any required input is null
     */
    private Set<TimeWindowOuter> doEventDetection( EventDetectionDetails details )
    {
        Objects.requireNonNull( details.dataset() );
        Objects.requireNonNull( details.featureGetter() );
        Objects.requireNonNull( details.featureGroup() );
        Objects.requireNonNull( details.eventRetriever() );
        Objects.requireNonNull( details.detection() );

        if ( details.dataset() == EventDetectionDataset.COVARIATES )
        {
            Objects.requireNonNull( details.covariateName() );
        }

        Set<TimeWindowOuter> events = new TreeSet<>();
        TimeWindowOuter unbounded = TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow() );
        FeatureGroup featureGroup = details.featureGroup();
        Function<FeatureTuple, Feature> featureGetter = details.featureGetter();
        RetrieverFactory<Double, Double, Double> eventRetriever = details.eventRetriever();

        switch ( details.dataset() )
        {
            case OBSERVED ->
            {
                Set<Feature> features = this.getFeatures( featureGroup.getFeatures(), featureGetter );
                Stream<TimeSeries<Double>> series = eventRetriever.getLeftRetriever( features )
                                                                  .get();

                Set<TimeWindowOuter> innerEvents = series.flatMap( s -> this.doEventDetection( s,
                                                                                               details,
                                                                                               this.leftUpscaler() )
                                                                            .stream() )
                                                         .collect( Collectors.toSet() );
                LOGGER.info( DETECTED_EVENTS_IN_THE_DATASET,
                             innerEvents.size(),
                             EventDetectionDataset.OBSERVED,
                             featureGroup.getName() );
                events.addAll( innerEvents );
            }
            case PREDICTED ->
            {
                Set<Feature> features = this.getFeatures( featureGroup.getFeatures(), featureGetter );
                Stream<TimeSeries<Double>> series = eventRetriever.getRightRetriever( features, unbounded )
                                                                  .get();

                Set<TimeWindowOuter> innerEvents = series.flatMap( s -> this.doEventDetection( s,
                                                                                               details,
                                                                                               this.rightUpscaler() )
                                                                            .stream() )
                                                         .collect( Collectors.toSet() );
                LOGGER.info( DETECTED_EVENTS_IN_THE_DATASET,
                             innerEvents.size(),
                             EventDetectionDataset.PREDICTED,
                             featureGroup.getName() );
                events.addAll( innerEvents );
            }
            case BASELINE ->
            {
                Set<Feature> features = this.getFeatures( featureGroup.getFeatures(), featureGetter );
                Stream<TimeSeries<Double>> series = eventRetriever.getBaselineRetriever( features, unbounded )
                                                                  .get();

                Set<TimeWindowOuter> innerEvents = series.flatMap( s -> this.doEventDetection( s,
                                                                                               details,
                                                                                               this.baselineUpscaler() )
                                                                            .stream() )
                                                         .collect( Collectors.toSet() );
                LOGGER.info( DETECTED_EVENTS_IN_THE_DATASET,
                             innerEvents.size(),
                             EventDetectionDataset.BASELINE,
                             featureGroup.getName() );
                events.addAll( innerEvents );
            }
            case COVARIATES ->
            {
                Set<Feature> features = this.getFeatures( featureGroup.getFeatures(), featureGetter );
                Stream<TimeSeries<Double>> series = eventRetriever.getCovariateRetriever( features,
                                                                                          details.covariateName() )
                                                                  .get();

                Set<TimeWindowOuter> innerEvents =
                        series.flatMap( s -> this.doEventDetection( s,
                                                                    this.getAdjustedDetails( details, s.getMetadata()
                                                                                                       .getUnit() ),
                                                                    this.covariateUpscaler() )
                                                 .stream() )
                              .collect( Collectors.toSet() );
                LOGGER.info( "Detected {} events in the {} dataset for feature group {} with variable name, '{}'.",
                             innerEvents.size(),
                             EventDetectionDataset.COVARIATES,
                             featureGroup.getName(),
                             details.covariateName() );
                events.addAll( innerEvents );
            }
        }

        return Collections.unmodifiableSet( events );
    }

    /**
     * Combines detected events using the prescribed {@link EventDetectionCombination} strategy, modifying the existing
     * set of events in place.
     *
     * @param combine is true to attempt combination, false otherwise
     * @param events the existing events
     * @param newEvents the new events
     * @param combination the combination strategy
     */

    private void combineEvents( boolean combine,
                                Set<TimeWindowOuter> events,
                                Set<TimeWindowOuter> newEvents,
                                EventDetectionCombination combination )
    {
        // First dataset, so add the events to the existing events only
        if ( !combine )
        {
            LOGGER.debug( "Skipping event combination as event detection has not yet been attempted, but adding "
                          + "events to the set of existing events." );
            events.addAll( newEvents );
        }
        // Union requested, else a covariate dataset and this is the first covariate discovered
        else if ( combination == EventDetectionCombination.UNION )
        {
            LOGGER.debug( "Combining detected events using the {}.", EventDetectionCombination.UNION );
            events.addAll( newEvents );
        }
        else if ( combination == EventDetectionCombination.INTERSECTION )
        {
            LOGGER.debug( "Combining detected events using the {}.", EventDetectionCombination.INTERSECTION );
            Set<TimeWindowOuter> intersection = TimeWindowSlicer.intersection( events, newEvents );
            events.clear();
            events.addAll( intersection );
        }
        else
        {
            throw new EventDetectionException( "Unrecognized method for combining detected events: "
                                               + combination
                                               + "." );
        }

        if ( combine
             && LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Upon combining events with the {}, identified {} joint events from a total of {} "
                          + "marginal events.",
                          combination,
                          events.size(),
                          events.size() + newEvents.size() );
        }
    }

    /**
     * Returns the aggregate of intersecting events.
     *
     * @param events the events
     * @param method the method
     * @param featureGroup the feature group
     * @return the aggregate of intersecting events
     */

    private Set<TimeWindowOuter> aggregateEvents( Set<TimeWindowOuter> events,
                                                  TimeWindowAggregation method,
                                                  FeatureGroup featureGroup )
    {
        // Short-circuit
        if ( Objects.isNull( method ) )
        {
            return events;
        }

        Set<TimeWindowOuter> aggregated = new HashSet<>();
        for ( TimeWindowOuter nextOuter : events )
        {
            Set<TimeWindowOuter> intersected = new HashSet<>();
            for ( TimeWindowOuter nextInner : events )
            {
                if ( !Objects.equals( nextOuter, nextInner )
                     && TimeWindowSlicer.intersects( nextOuter, nextInner ) )
                {
                    intersected.add( nextOuter );
                    intersected.add( nextInner );
                }
            }
            TimeWindowOuter aggregate = TimeWindowSlicer.aggregate( intersected, method );
            aggregated.add( aggregate );
        }

        LOGGER.info( "Following aggregation of the detected events for feature group {}, produced {} aggregated events "
                     + "from the {} detected events.", featureGroup.getName(), aggregated.size(), events.size() );

        return Collections.unmodifiableSet( aggregated );
    }

    /**
     * Performs event detection.
     *
     * @param timeSeries the time-series
     * @param details the event detection details
     * @return the time windows, one for each detected event
     */

    private Set<TimeWindowOuter> doEventDetection( TimeSeries<Double> timeSeries,
                                                   EventDetectionDetails details,
                                                   TimeSeriesUpscaler<Double> upscaler )
    {
        // Upscale the time-series if needed
        boolean upscale = Objects.nonNull( details.desiredTimeScale() )
                          && Objects.nonNull( timeSeries.getTimeScale() )
                          && TimeScaleOuter.isRescalingRequired( timeSeries.getTimeScale(),
                                                                 details.desiredTimeScale() )
                          && !details.desiredTimeScale()
                                     .equals( timeSeries.getTimeScale() );

        if ( upscale )
        {
            LOGGER.debug( "Upscaling a time-series with {} events for event detection. The time-series has the "
                          + "following metadata: {}.",
                          timeSeries.getEvents()
                                    .size(),
                          timeSeries.getMetadata() );
            RescaledTimeSeriesPlusValidation<Double> upscaled = upscaler.upscale( timeSeries,
                                                                                  details.desiredTimeScale(),
                                                                                  details.measurementUnit() );

            // Log any warnings
            RescaledTimeSeriesPlusValidation.logScaleValidationWarnings( timeSeries,
                                                                         upscaled.getValidationEvents() );

            timeSeries = upscaled.getTimeSeries();
        }

        LOGGER.debug( "Performing event detection of a time-series dataset containing {} events and the following "
                      + "metadata: {}.", timeSeries.getEvents()
                                                   .size(), timeSeries.getMetadata() );

        // Unbounded time window, placeholder
        return this.eventDetector()
                   .detect( timeSeries );
    }

    /**
     * Adjusts the desired timescale, inserting the function provided, if available.
     * @param desiredTimeScale the desired timescale
     * @param function the function to insert, where defined
     * @return the adjusted timescale
     */
    private TimeScaleOuter getAdjustedTimeScale( TimeScaleOuter desiredTimeScale,
                                                 TimeScale.TimeScaleFunction function )
    {
        if ( Objects.isNull( function )
             || Objects.isNull( desiredTimeScale ) )
        {
            return desiredTimeScale;
        }

        return TimeScaleOuter.of( desiredTimeScale.getTimeScale()
                                                  .toBuilder()
                                                  .setFunction( function )
                                                  .build() );
    }

    /**
     * Adjusts the supplied details for event detection to include a new measurement unit.
     * @param details the existing details
     * @param measurementUnit the new measurement unit
     * @return the adjusted details, including the new measurement unit
     */
    private EventDetectionDetails getAdjustedDetails( EventDetectionDetails details, String measurementUnit )
    {
        return new EventDetectionDetails( details.dataset(),
                                          details.featureGroup(),
                                          details.featureGetter(),
                                          details.eventRetriever(),
                                          details.detection(),
                                          details.covariateName(),
                                          details.desiredTimeScale(),
                                          details.upscaler(),
                                          measurementUnit );
    }

    /**
     * @param featureTuples the feature tuples
     * @param featureGetter the feature-getter
     * @return the features using the prescribed feature-getter
     * @throws NullPointerException if the metadata is null
     */

    private Set<Feature> getFeatures( Set<FeatureTuple> featureTuples, Function<FeatureTuple, Feature> featureGetter )
    {
        Objects.requireNonNull( featureGetter );
        Objects.requireNonNull( featureTuples );

        return featureTuples.stream()
                            .map( featureGetter )
                            .collect( Collectors.toUnmodifiableSet() );
    }

    /**
     * Details for event detection.
     * @param dataset the dataset
     * @param featureGroup the feature group
     * @param featureGetter the feature getter
     * @param eventRetriever the time-series retriever factory
     * @param detection the detection parameters
     * @param covariateName the covariate name, required for a covariate dataset
     * @param desiredTimeScale the desired timescale
     * @param upscaler the upscaler
     * @param measurementUnit the measurement unit
     */

    private record EventDetectionDetails( EventDetectionDataset dataset,
                                          FeatureGroup featureGroup,
                                          Function<FeatureTuple, Feature> featureGetter,
                                          RetrieverFactory<Double, Double, Double> eventRetriever,
                                          EventDetection detection,
                                          String covariateName,
                                          TimeScaleOuter desiredTimeScale,
                                          TimeSeriesUpscaler<Double> upscaler,
                                          String measurementUnit )
    {
    }

}
