package wres.statistics;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import com.google.flatbuffers.FlatBufferBuilder;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.Ensemble;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.statistics.generated.flatbuffers.DiagramMetric;
import wres.statistics.generated.flatbuffers.DiagramMetric_.DiagramMetricComponent_.DiagramComponentName;
import wres.statistics.generated.flatbuffers.DiagramMetric_.DiagramMetricComponent;
import wres.statistics.generated.flatbuffers.DiagramStatistic;
import wres.statistics.generated.flatbuffers.Duration;
import wres.statistics.generated.flatbuffers.DiagramStatistic_.DiagramStatisticComponent;
import wres.statistics.generated.flatbuffers.Geometry;
import wres.statistics.generated.flatbuffers.ReferenceTime_.ReferenceTimeType;
import wres.statistics.generated.flatbuffers.Pool;
import wres.statistics.generated.flatbuffers.ScoreMetric;
import wres.statistics.generated.flatbuffers.ScoreMetric_.ScoreMetricComponent;
import wres.statistics.generated.flatbuffers.ScoreMetric_.ScoreMetricComponent_.ScoreComponentName;
import wres.statistics.generated.flatbuffers.ScoreStatistic_.ScoreStatisticComponent;
import wres.statistics.generated.flatbuffers.ScoreStatistic;
import wres.statistics.generated.flatbuffers.Statistics;
import wres.statistics.generated.flatbuffers.Threshold;
import wres.statistics.generated.flatbuffers.MetricName;
import wres.statistics.generated.flatbuffers.Pairs;
import wres.statistics.generated.flatbuffers.ReferenceTime;
import wres.statistics.generated.flatbuffers.Pairs_.TimeSeriesOfPairs;
import wres.statistics.generated.flatbuffers.Threshold_.ThresholdType;
import wres.statistics.generated.flatbuffers.TimeWindow;
import wres.statistics.generated.flatbuffers.Timestamp;

/**
 * Creates statistics messages in flatbuffer format from internal representations.
 * 
 * @author james.brown@hydrosolved.com
 */

public class FlatbuffersMessageFactory
{

    /**
     * Creates a {@link wres.statistics.generated.Statistics} from a
     * {@link wres.datamodel.statistics.StatisticsForProject}.
     * 
     * @param builder the builder
     * @param project the project statistics
     * @param pairs the ensemble pairs
     * @return the offset at which the message component was created
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if there are zero statistics in total
     * @throws InterruptedException if the statistics could not be retrieved from the project
     */

    public static int parse( FlatBufferBuilder builder,
                             StatisticsForProject project,
                             PoolOfPairs<Double, Ensemble> pairs )
            throws InterruptedException
    {
        Objects.requireNonNull( project );
        Objects.requireNonNull( builder );

        SampleMetadata metadata = SampleMetadata.of();

        // Add the double scores
        int scoresOffset = -1;
        if ( project.hasStatistic( StatisticType.DOUBLE_SCORE ) )
        {
            List<wres.datamodel.statistics.DoubleScoreStatistic> doubleScores = project.getDoubleScoreStatistics();
            int[] statistics = new int[doubleScores.size()];
            for ( int i = 0; i < statistics.length; i++ )
            {
                statistics[i] = FlatbuffersMessageFactory.parse( builder, doubleScores.get( i ) );
            }

            scoresOffset = Statistics.createScoresVector( builder, statistics );

            metadata = doubleScores.get( 0 ).getMetadata().getSampleMetadata();
        }

        // Add the diagrams
        int diagramsOffset = -1;
        if ( project.hasStatistic( StatisticType.DIAGRAM ) )
        {
            List<wres.datamodel.statistics.DiagramStatistic> diagrams = project.getDiagramStatistics();
            int[] statistics = new int[diagrams.size()];
            for ( int i = 0; i < statistics.length; i++ )
            {
                statistics[i] = FlatbuffersMessageFactory.parse( builder, diagrams.get( i ) );
            }

            diagramsOffset = Statistics.createDiagramsVector( builder, statistics );

            metadata = diagrams.get( 0 ).getMetadata().getSampleMetadata();
        }

        int poolOffset = FlatbuffersMessageFactory.parse( builder, metadata, pairs );

        Statistics.startStatistics( builder );

        if ( scoresOffset > -1 )
        {
            Statistics.addScores( builder, scoresOffset );
        }

        if ( diagramsOffset > -1 )
        {
            Statistics.addDiagrams( builder, diagramsOffset );
        }

        Statistics.addPool( builder, poolOffset );

        return Statistics.endStatistics( builder );
    }

    /**
     * Creates a {@link wres.statistics.generated.flatbuffers.ScoreStatistic} from a 
     * {@link wres.datamodel.statistics.DoubleScoreStatistic}.
     * 
     * @param builder the builder
     * @param statistic the statistic from which to create a message
     * @return the offset at which the message component was created
     * @throws NullPointerException if any input is null
     */

    private static int parse( FlatBufferBuilder builder,
                              wres.datamodel.statistics.DoubleScoreStatistic statistic )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( statistic );

        MetricConstants metricName = statistic.getMetadata().getMetricID();

        String unitString = statistic.getMetadata().getMeasurementUnit().toString();

        int units = builder.createString( unitString );

        SampleMetadata metadata = statistic.getMetadata().getSampleMetadata();
        int eventThreshold = -1;
        int decisionThreshold = -1;
        if ( metadata.hasThresholds() )
        {
            OneOrTwoThresholds thresholds = metadata.getThresholds();
            eventThreshold = FlatbuffersMessageFactory.parse( builder, thresholds.first() );

            if ( thresholds.hasTwo() )
            {
                decisionThreshold = FlatbuffersMessageFactory.parse( builder, thresholds.second() );
            }
        }

        // Set the metric components and score values
        // and then propagate to the payload here
        int[] metricComponents = new int[statistic.getComponents().size()];
        int[] statisticComponents = new int[statistic.getComponents().size()];

        int componentCount = 0;
        for ( MetricConstants next : statistic.getComponents() )
        {
            int componentName = FlatbuffersMessageFactory.getScoreComponentName( next );

            Double minimum = (Double) metricName.getMinimum();
            Double maximum = (Double) metricName.getMaximum();
            Double optimum = (Double) metricName.getOptimum();

            // Set the limits for the component where available
            if ( next.hasLimits() )
            {
                minimum = (Double) next.getMinimum();
                maximum = (Double) next.getMaximum();
                optimum = (Double) next.getOptimum();
            }

            int component = ScoreMetricComponent.createScoreMetricComponent( builder,
                                                                             componentName,
                                                                             units,
                                                                             optimum,
                                                                             minimum,
                                                                             maximum );
            metricComponents[componentCount] = component;

            double value = statistic.getComponent( next ).getData();
            int statisticComponent =
                    ScoreStatisticComponent.createScoreStatisticComponent( builder, componentName, value );
            statisticComponents[componentCount] = statisticComponent;

            componentCount++;
        }

        int metricComponentsOffset = ScoreMetric.createComponentsVector( builder, metricComponents );
        int statisticComponentsOffset = ScoreStatistic.createStatisticsVector( builder, statisticComponents );

        int name = FlatbuffersMessageFactory.getMetricName( metricName );

        int scoreMetric = ScoreMetric.createScoreMetric( builder, name, metricComponentsOffset );

        ScoreStatistic.startScoreStatistic( builder );

        if ( eventThreshold > -1 )
        {
            ScoreStatistic.addEventThreshold( builder, eventThreshold );
        }

        if ( decisionThreshold > -1 )
        {
            ScoreStatistic.addDecisionThreshold( builder, decisionThreshold );
        }

        ScoreStatistic.addMetric( builder, scoreMetric );
        ScoreStatistic.addStatistics( builder, statisticComponentsOffset );

        return ScoreStatistic.endScoreStatistic( builder );
    }


    /**
     * Creates a {@link wres.statistics.generated.flatbuffers.DiagramStatistic} from a 
     * {@link wres.datamodel.statistics.DiagramStatistic}.
     * 
     * @param builder the builder
     * @param statistic the statistic from which to create a message
     * @return the offset at which the message component was created
     * @throws NullPointerException if any input is null
     */

    private static int parse( FlatBufferBuilder builder,
                              wres.datamodel.statistics.DiagramStatistic statistic )
    {
        Objects.requireNonNull( statistic );
        Objects.requireNonNull( builder );

        MetricConstants metricName = statistic.getMetadata().getMetricID();

        String unitString = statistic.getMetadata().getMeasurementUnit().toString();

        int units = builder.createString( unitString );

        SampleMetadata metadata = statistic.getMetadata().getSampleMetadata();
        int eventThreshold = -1;
        int decisionThreshold = -1;
        if ( metadata.hasThresholds() )
        {
            OneOrTwoThresholds thresholds = metadata.getThresholds();
            eventThreshold = FlatbuffersMessageFactory.parse( builder, thresholds.first() );

            if ( thresholds.hasTwo() )
            {
                decisionThreshold = FlatbuffersMessageFactory.parse( builder, thresholds.second() );
            }
        }

        // Set the metric components and score values
        // and then propagate to the payload here
        int[] metricComponents = new int[statistic.getData().size()];
        int[] statisticComponents = new int[statistic.getData().size()];

        int componentCount = 0;
        for ( Map.Entry<MetricDimension, VectorOfDoubles> next : statistic.getData().entrySet() )
        {
            MetricDimension nextDimension = next.getKey();

            int componentName = FlatbuffersMessageFactory.getDiagramDimension( nextDimension );

            DiagramMetricComponent.startDiagramMetricComponent( builder );

            DiagramMetricComponent.addName( builder, componentName );

            DiagramMetricComponent.addUnits( builder, units );

            metricComponents[componentCount] = DiagramMetricComponent.endDiagramMetricComponent( builder );

            double[] values = next.getValue().getDoubles();

            int valuesOffset = DiagramStatisticComponent.createValuesVector( builder, values );

            int statisticComponent =
                    DiagramStatisticComponent.createDiagramStatisticComponent( builder, componentName, valuesOffset );

            statisticComponents[componentCount] = statisticComponent;

            componentCount++;
        }

        int metricComponentsOffset = DiagramMetric.createComponentsVector( builder, metricComponents );
        int statisticComponentsOffset = DiagramStatistic.createStatisticsVector( builder, statisticComponents );

        int name = FlatbuffersMessageFactory.getMetricName( metricName );

        int diagramMetric = DiagramMetric.createDiagramMetric( builder, name, metricComponentsOffset );

        DiagramStatistic.startDiagramStatistic( builder );

        if ( eventThreshold > -1 )
        {
            ScoreStatistic.addEventThreshold( builder, eventThreshold );
        }

        if ( decisionThreshold > -1 )
        {
            ScoreStatistic.addDecisionThreshold( builder, decisionThreshold );
        }

        DiagramStatistic.addMetric( builder, diagramMetric );
        DiagramStatistic.addStatistics( builder, statisticComponentsOffset );

        return DiagramStatistic.endDiagramStatistic( builder );
    }

    /**
     * Returns the offset corresponding to a prescribed {@link wres.datamodel.MetricConstants}.
     * 
     * @param metricName the metric name
     * @return the offset
     * @throws UnsupportedOperationException if the metric name is unrecognized
     * @throws NullPointerException if the input is null
     */

    private static int getMetricName( MetricConstants metricName )
    {
        Objects.requireNonNull( metricName );

        switch ( metricName )
        {
            case BIAS_FRACTION:
                return MetricName.BIAS_FRACTION;
            case BOX_PLOT_OF_ERRORS:
                return MetricName.BOX_PLOT_OF_ERRORS;
            case BOX_PLOT_OF_PERCENTAGE_ERRORS:
                return MetricName.BOX_PLOT_OF_PERCENTAGE_ERRORS;
            case BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE:
                return MetricName.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE;
            case BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE:
                return MetricName.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE;
            case BRIER_SCORE:
                return MetricName.BRIER_SCORE;
            case BRIER_SKILL_SCORE:
                return MetricName.BRIER_SKILL_SCORE;
            case COEFFICIENT_OF_DETERMINATION:
                return MetricName.COEFFICIENT_OF_DETERMINATION;
            case CONTINGENCY_TABLE:
                return MetricName.CONTINGENCY_TABLE;
            case PEARSON_CORRELATION_COEFFICIENT:
                return MetricName.PEARSON_CORRELATION_COEFFICIENT;
            case CONTINUOUS_RANKED_PROBABILITY_SCORE:
                return MetricName.CONTINUOUS_RANKED_PROBABILITY_SCORE;
            case CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE:
                return MetricName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE;
            case THREAT_SCORE:
                return MetricName.THREAT_SCORE;
            case EQUITABLE_THREAT_SCORE:
                return MetricName.EQUITABLE_THREAT_SCORE;
            case FREQUENCY_BIAS:
                return MetricName.FREQUENCY_BIAS;
            case INDEX_OF_AGREEMENT:
                return MetricName.INDEX_OF_AGREEMENT;
            case KLING_GUPTA_EFFICIENCY:
                return MetricName.KLING_GUPTA_EFFICIENCY;
            case MEAN_ABSOLUTE_ERROR:
                return MetricName.MEAN_ABSOLUTE_ERROR;
            case MEAN_ERROR:
                return MetricName.MEAN_ERROR;
            case MEAN_SQUARE_ERROR:
                return MetricName.MEAN_SQUARE_ERROR;
            case MEAN_SQUARE_ERROR_SKILL_SCORE:
                return MetricName.MEAN_SQUARE_ERROR_SKILL_SCORE;
            case MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED:
                return MetricName.MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED;
            case MEDIAN_ERROR:
                return MetricName.MEDIAN_ERROR;
            case PEIRCE_SKILL_SCORE:
                return MetricName.PEIRCE_SKILL_SCORE;
            case PROBABILITY_OF_DETECTION:
                return MetricName.PROBABILITY_OF_DETECTION;
            case PROBABILITY_OF_FALSE_DETECTION:
                return MetricName.PROBABILITY_OF_FALSE_DETECTION;
            case QUANTILE_QUANTILE_DIAGRAM:
                return MetricName.QUANTILE_QUANTILE_DIAGRAM;
            case RANK_HISTOGRAM:
                return MetricName.RANK_HISTOGRAM;
            case RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM:
                return MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM;
            case RELATIVE_OPERATING_CHARACTERISTIC_SCORE:
                return MetricName.RELATIVE_OPERATING_CHARACTERISTIC_SCORE;
            case RELIABILITY_DIAGRAM:
                return MetricName.RELIABILITY_DIAGRAM;
            case ROOT_MEAN_SQUARE_ERROR:
                return MetricName.ROOT_MEAN_SQUARE_ERROR;
            case ROOT_MEAN_SQUARE_ERROR_NORMALIZED:
                return MetricName.ROOT_MEAN_SQUARE_ERROR_NORMALIZED;
            case SAMPLE_SIZE:
                return MetricName.SAMPLE_SIZE;
            case SUM_OF_SQUARE_ERROR:
                return MetricName.SUM_OF_SQUARE_ERROR;
            case VOLUMETRIC_EFFICIENCY:
                return MetricName.VOLUMETRIC_EFFICIENCY;
            default:
                throw new UnsupportedOperationException( "Unknown metric: " + metricName );
        }
    }

    /**
     * Creates a {@link wres.statistics.generated.flatbuffers.Threshold} from a 
     * {@link wres.datamodel.thresholds.Threshold}.
     * 
     * @param builder the builder
     * @param threshold the threshold from which to create a message
     * @return the offset at which the message component was created
     * @throws NullPointerException if any input is null
     */

    private static int parse( FlatBufferBuilder builder,
                              wres.datamodel.thresholds.Threshold threshold )
    {
        Objects.requireNonNull( builder );

        Objects.requireNonNull( threshold );

        int unitsOffset = -1;

        if ( threshold.hasUnits() )
        {
            unitsOffset = builder.createString( threshold.getUnits().getUnit() );
        }

        int nameOffset = -1;

        if ( threshold.hasLabel() )
        {
            nameOffset = builder.createString( threshold.getLabel() );
        }

        int operator = FlatbuffersMessageFactory.getThresholdOperatorType( threshold.getCondition() );

        Threshold.startThreshold( builder );

        Threshold.addType( builder, operator );

        if ( threshold.hasProbabilities() )
        {
            Threshold.addLeftThresholdProbability( builder, threshold.getProbabilities().first() );
        }

        Threshold.addLeftThresholdValue( builder, threshold.getValues().first() );

        if ( threshold.hasBetweenCondition() )
        {
            if ( threshold.hasProbabilities() )
            {
                Threshold.addRightThresholdProbability( builder, threshold.getProbabilities().second() );
            }

            Threshold.addRightThresholdValue( builder, threshold.getValues().second() );
        }

        if ( threshold.hasLabel() )
        {
            Threshold.addName( builder, nameOffset );
        }

        if ( threshold.hasUnits() )
        {
            Threshold.addThresholdValueUnits( builder, unitsOffset );
        }

        return Threshold.endThreshold( builder );
    }

    /**
     * Returns the offset corresponding to a prescribed {@link wres.datamodel.threshold.Operator}.
     * 
     * @param operator the threshold operator
     * @return the offset
     * @throws UnsupportedOperationException if the operator is unrecognized
     * @throws NullPointerException if the input is null
     */

    private static int getThresholdOperatorType( Operator operator )
    {
        Objects.requireNonNull( operator );

        switch ( operator )
        {
            case LESS:
                return ThresholdType.LESS;
            case GREATER:
                return ThresholdType.GREATER;
            case LESS_EQUAL:
                return ThresholdType.LESS_EQUAL;
            case GREATER_EQUAL:
                return ThresholdType.GREATER_EQUAL;
            case BETWEEN:
                return ThresholdType.BETWEEN;
            default:
                throw new UnsupportedOperationException( "Unknown threshold operator type: " + operator );
        }
    }

    /**
     * Returns the offset corresponding to a prescribed {@link wres.datamodel.MetricConstants.MetricDimension}.
     * 
     * @param dimension the diagram dimension
     * @return the offset
     * @throws UnsupportedOperationException if the operator is unrecognized
     * @throws NullPointerException if the input is null
     */

    private static int getDiagramDimension( MetricDimension dimension )
    {
        Objects.requireNonNull( dimension );

        switch ( dimension )
        {
            case PROBABILITY_OF_DETECTION:
                return DiagramComponentName.PROBABILITY_OF_DETECTION;
            case PROBABILITY_OF_FALSE_DETECTION:
                return DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION;
            case FORECAST_PROBABILITY:
                return DiagramComponentName.FORECAST_PROBABILITY;
            case OBSERVED_RELATIVE_FREQUENCY:
                return DiagramComponentName.OBSERVED_RELATIVE_FREQUENCY;
            case OBSERVED_QUANTILES:
                return DiagramComponentName.OBSERVED_QUANTILES;
            case PREDICTED_QUANTILES:
                return DiagramComponentName.PREDICTED_QUANTILES;
            case RANK_ORDER:
                return DiagramComponentName.RANK_ORDER;
            case SAMPLE_SIZE:
                return DiagramComponentName.SAMPLE_SIZE;
            default:
                throw new UnsupportedOperationException( "Unknown diagram dimension: " + dimension );
        }
    }

    /**
     * Returns the offset corresponding to a prescribed {@link wres.datamodel.MetricConstants}.
     * 
     * @param scoreComponentName the score component name
     * @return the offset
     * @throws UnsupportedOperationException if the reference time type is unrecognized
     * @throws NullPointerException if the input is null
     */

    private static int getScoreComponentName( MetricConstants scoreComponentName )
    {
        Objects.requireNonNull( scoreComponentName );

        switch ( scoreComponentName )
        {
            case MAIN:
                return ScoreComponentName.MAIN_SCORE;
            case RELIABILITY:
                return ScoreComponentName.RELIABILITY;
            case RESOLUTION:
                return ScoreComponentName.RESOLUTION;
            case UNCERTAINTY:
                return ScoreComponentName.UNCERTAINTY;
            case TYPE_II_CONDITIONAL_BIAS:
                return ScoreComponentName.TYPE_II_BIAS;
            case DISCRIMINATION:
                return ScoreComponentName.DISCRIMINATION;
            case SHARPNESS:
                return ScoreComponentName.SHARPNESS;
            case POTENTIAL:
                return ScoreComponentName.POTENTIAL;
            default:
                throw new UnsupportedOperationException( "Unknown score component type: " + scoreComponentName );
        }
    }

    /**
     * Creates a {@link wres.statistics.generated.flatbuffers.Pool} from a 
     * {@link wres.datamodel.sampledata.SampleMetadata} and a {@link PoolOfPairs}.
     * 
     * @param builder the builder
     * @param metadata the metadata
     * @param ensemblePairs the ensemble pairs
     * @return the offset at which the message component was created
     * @throws NullPointerException if any input is null
     */

    private static int parse( FlatBufferBuilder builder,
                              wres.datamodel.sampledata.SampleMetadata metadata,
                              PoolOfPairs<Double, Ensemble> ensemblePairs )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( ensemblePairs );

        DatasetIdentifier identifier = metadata.getIdentifier();

        int geometries = -1;

        // Up to one geometry for now
        if ( identifier.hasGeospatialID() )
        {
            int geometry = FlatbuffersMessageFactory.parse( builder, identifier.getGeospatialID() );
            geometries = Pool.createGeometriesVector( builder, new int[] { geometry } );
        }

        int timeWindow = -1;

        if ( metadata.hasTimeWindow() )
        {
            timeWindow = FlatbuffersMessageFactory.parse( builder, metadata.getTimeWindow() );
        }

        int pairs = FlatbuffersMessageFactory.parseEnsemblePairs( builder, ensemblePairs );

        Pool.startPool( builder );

        if ( identifier.hasGeospatialID() )
        {
            Pool.addGeometries( builder, geometries );
        }

        if ( metadata.hasTimeWindow() )
        {
            Pool.addTimeWindow( builder, timeWindow );
        }

        Pool.addPairs( builder, pairs );

        return Pool.endPool( builder );
    }

    /**
     * Creates a {@link wres.statistics.generated.flatbuffers.TimeWindow} from a 
     * {@link wres.datamodel.time.TimeWindow}.
     * 
     * @param builder the builder
     * @param timeWindow the time window from which to create a message
     * @return the offset at which the message component was created
     */

    private static int parse( FlatBufferBuilder builder, wres.datamodel.time.TimeWindow timeWindow )
    {
        Objects.requireNonNull( timeWindow );
        Objects.requireNonNull( builder );

        Instant earliestValid = timeWindow.getEarliestValidTime();

        int earliestValidTime = Timestamp.createTimestamp( builder,
                                                           earliestValid.getEpochSecond(),
                                                           earliestValid.getNano() );

        Instant latestValid = timeWindow.getLatestValidTime();

        int latestValidTime = Timestamp.createTimestamp( builder,
                                                         latestValid.getEpochSecond(),
                                                         latestValid.getNano() );

        Instant earliestReference = timeWindow.getEarliestReferenceTime();

        int earliestReferenceTime = Timestamp.createTimestamp( builder,
                                                               earliestReference.getEpochSecond(),
                                                               earliestReference.getNano() );

        Instant latestReference = timeWindow.getLatestReferenceTime();

        int latestReferenceTime = Timestamp.createTimestamp( builder,
                                                             latestReference.getEpochSecond(),
                                                             latestReference.getNano() );

        java.time.Duration earliestLead = timeWindow.getEarliestLeadDuration();

        int earliestLeadDuration = Duration.createDuration( builder,
                                                            earliestLead.getSeconds(),
                                                            earliestLead.getNano() );

        java.time.Duration latestLead = timeWindow.getLatestLeadDuration();

        int latestLeadDuration = Duration.createDuration( builder,
                                                          latestLead.getSeconds(),
                                                          latestLead.getNano() );

        int referenceTimeType =
                TimeWindow.createReferenceTimeTypeVector( builder, new int[] { ReferenceTimeType.UNKNOWN } );

        return TimeWindow.createTimeWindow( builder,
                                            earliestReferenceTime,
                                            latestReferenceTime,
                                            earliestValidTime,
                                            latestValidTime,
                                            earliestLeadDuration,
                                            latestLeadDuration,
                                            referenceTimeType );
    }

    /**
     * Creates a {@link wres.statistics.generated.flatbuffers.Geometry} from a 
     * {@link wres.datamodel.sampledata.Location}.
     * 
     * @param builder the builder
     * @param location the location from which to create a message
     * @return the offset at which the message component was created
     */

    private static int parse( FlatBufferBuilder builder, wres.datamodel.sampledata.Location location )
    {
        Objects.requireNonNull( location );
        Objects.requireNonNull( builder );

        if ( location.hasLocationName() )
        {
            // Create the location name
            String name = location.getLocationName();
            int nameOffset = builder.createString( name );

            // Start the geometry
            Geometry.startGeometry( builder );
            Geometry.addName( builder, nameOffset );
        }
        else
        {
            Geometry.startGeometry( builder );
        }

        if ( location.hasCoordinates() )
        {
            Geometry.addLatitude( builder, location.getLatitude() );
            Geometry.addLongitude( builder, location.getLongitude() );
        }

        return Geometry.endGeometry( builder );
    }

    /**
    * Creates a {@link Pairs} from a {@link PoolOfPairs}.
    * 
    * @param builder the builder
    * @param pairs the pairs
    * @return the offset at which the message component was created 
    */

    private static int parseEnsemblePairs( FlatBufferBuilder builder, PoolOfPairs<Double, Ensemble> pairs )
    {
        Objects.requireNonNull( pairs );
        Objects.requireNonNull( builder );

        int[] series = new int[pairs.get().size()];
        int seriesCount = 0;
        for ( TimeSeries<Pair<Double, Ensemble>> nextSeries : pairs.get() )
        {
            // Add the reference times
            Map<wres.datamodel.time.ReferenceTimeType, Instant> times = nextSeries.getReferenceTimes();
            int[] referenceTimes = new int[times.size()];
            int refTimes = 0;
            for ( Map.Entry<wres.datamodel.time.ReferenceTimeType, Instant> nextEntry : times.entrySet() )
            {
                wres.datamodel.time.ReferenceTimeType nextType = nextEntry.getKey();
                Instant nextTime = nextEntry.getValue();

                int referenceTime = Timestamp.createTimestamp( builder, nextTime.getEpochSecond(), nextTime.getNano() );
                int referenceTimeType = FlatbuffersMessageFactory.getOffsetForReferenceTimeType( nextType );

                int nextReferenceTime = ReferenceTime.createReferenceTime( builder, referenceTime, referenceTimeType );
                referenceTimes[refTimes] = nextReferenceTime;
                refTimes++;
            }

            int referenceTimeOffset = TimeSeriesOfPairs.createReferenceTimesVector( builder, referenceTimes );

            // Add the events
            int[] ensemblePairs = new int[nextSeries.getEvents().size()];
            int ensPairs = 0;
            for ( Event<Pair<Double, Ensemble>> nextEvent : nextSeries.getEvents() )
            {

                Double leftSide = nextEvent.getValue().getLeft();
                double[] rightSide = nextEvent.getValue().getRight().getMembers();

                int validTime = Timestamp.createTimestamp( builder,
                                                           nextEvent.getTime().getEpochSecond(),
                                                           nextEvent.getTime().getNano() );

                int left =
                        wres.statistics.generated.flatbuffers.Pairs_.Pair.createLeftVector( builder,
                                                                                            new double[] { leftSide } );

                int right =
                        wres.statistics.generated.flatbuffers.Pairs_.Pair.createRightVector( builder, rightSide );

                int nextPair =
                        wres.statistics.generated.flatbuffers.Pairs_.Pair.createPair( builder, validTime, left, right );

                ensemblePairs[ensPairs] = nextPair;

                ensPairs++;
            }

            int pairsOffset = TimeSeriesOfPairs.createPairsVector( builder, ensemblePairs );

            int seriesOffset = TimeSeriesOfPairs.createTimeSeriesOfPairs( builder, referenceTimeOffset, pairsOffset );

            series[seriesCount] = seriesOffset;

            seriesCount++;
        }

        int timeSeriesOffset = Pairs.createTimeSeriesVector( builder, series );

        return Pairs.createPairs( builder, timeSeriesOffset );
    }

    /**
     * Returns the offset corresponding to a prescribed {@link wres.datamodel.time.ReferenceTimeType}.
     * 
     * @param referenceTimeType the reference time type
     * @return the offset
     * @throws UnsupportedOperationException if the reference time type is unrecognized
     * @throws NullPointerException if the input is null
     */

    private static int getOffsetForReferenceTimeType( wres.datamodel.time.ReferenceTimeType referenceTimeType )
    {
        Objects.requireNonNull( referenceTimeType );

        switch ( referenceTimeType )
        {
            case UNKNOWN:
                return ReferenceTimeType.UNKNOWN;
            case T0:
                return ReferenceTimeType.T0;
            case ANALYSIS_START_TIME:
                return ReferenceTimeType.ANALYSIS_START_TIME;
            case ISSUED_TIME:
                return ReferenceTimeType.ISSUED_TIME;
            default:
                throw new UnsupportedOperationException( "Unknown reference time type: " + referenceTimeType );
        }
    }

    /**
     * Do not construct.
     */

    private FlatbuffersMessageFactory()
    {
    }

}
