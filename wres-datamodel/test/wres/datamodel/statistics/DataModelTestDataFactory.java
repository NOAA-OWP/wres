package wres.datamodel.statistics;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Factory class for generating test datasets for metric calculations.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class DataModelTestDataFactory
{

    /**
     * Second time for testing.
     */

    private static final String SECOND_TIME = "2010-12-31T11:59:59Z";

    /**
     * First time for testing.
     */

    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";

    private static final FeatureKey DRRC2 = FeatureKey.of( "DRRC2" );
    private static final FeatureTuple DRRC2_TUPLE = new FeatureTuple( DRRC2, DRRC2, DRRC2 );

    /**
     * Returns a {@link List} of {@link ScoreStatistic} comprising the MAE for selected
     * thresholds and forecast lead times using fake data.
     * 
     * @return an output map of verification scores
     */

    public static List<DoubleScoreStatisticOuter> getScalarMetricOutputTwo()
    {

        List<DoubleScoreStatisticOuter> statistics = new ArrayList<>();

        //Fake metadata
        final SampleMetadata source = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         DatasetIdentifier.of( DRRC2_TUPLE,
                                                                               "SQIN",
                                                                               "HEFS",
                                                                               "ESP" ) );

        int[] leadTimes = new int[] { 1, 2, 3, 4, 5 };

        //Iterate through the lead times
        for ( int leadTime : leadTimes )
        {
            final TimeWindowOuter timeWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                                   Instant.parse( SECOND_TIME ),
                                                                   Duration.ofHours( leadTime ) );

            // Add first result
            OneOrTwoThresholds first =
                    OneOrTwoThresholds.of( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                               OneOrTwoDoubles.of( 0.1 ),
                                                                               Operator.GREATER,
                                                                               ThresholdDataType.LEFT ),
                                           ThresholdOuter.of( OneOrTwoDoubles.of( 5.0 ),
                                                              Operator.GREATER,
                                                              ThresholdDataType.LEFT ) );

            DoubleScoreStatistic one =
                    DoubleScoreStatistic.newBuilder()
                                        .setMetric( DoubleScoreMetric.newBuilder()
                                                                     .setName( MetricName.MEAN_ABSOLUTE_ERROR ) )
                                        .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                     .setValue( 66.0 )
                                                                                     .setName( ComponentName.MAIN ) )
                                        .build();

            DoubleScoreStatisticOuter firstValue =
                    DoubleScoreStatisticOuter.of( one,
                                                  StatisticMetadata.of( SampleMetadata.of( source, timeWindow, first ),
                                                                        1000,
                                                                        MeasurementUnit.of(),
                                                                        MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                                        MetricConstants.MAIN ) );

            statistics.add( firstValue );


            // Add second result
            OneOrTwoThresholds second =
                    OneOrTwoThresholds.of( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 2.0 ),
                                                                               OneOrTwoDoubles.of( 0.2 ),
                                                                               Operator.GREATER,
                                                                               ThresholdDataType.LEFT ),
                                           ThresholdOuter.of( OneOrTwoDoubles.of( 5.0 ),
                                                              Operator.GREATER,
                                                              ThresholdDataType.LEFT ) );

            DoubleScoreStatistic two =
                    DoubleScoreStatistic.newBuilder()
                                        .setMetric( DoubleScoreMetric.newBuilder()
                                                                     .setName( MetricName.MEAN_ABSOLUTE_ERROR ) )
                                        .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                     .setValue( 67.0 )
                                                                                     .setName( ComponentName.MAIN ) )
                                        .build();

            DoubleScoreStatisticOuter secondValue =
                    DoubleScoreStatisticOuter.of( two,
                                                  StatisticMetadata.of( SampleMetadata.of( source, timeWindow, second ),
                                                                        1000,
                                                                        MeasurementUnit.of(),
                                                                        MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                                        MetricConstants.MAIN ) );

            statistics.add( secondValue );


            // Add third result
            OneOrTwoThresholds third =
                    OneOrTwoThresholds.of( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 3.0 ),
                                                                               OneOrTwoDoubles.of( 0.3 ),
                                                                               Operator.GREATER,
                                                                               ThresholdDataType.LEFT ),
                                           ThresholdOuter.of( OneOrTwoDoubles.of( 6.0 ),
                                                              Operator.GREATER,
                                                              ThresholdDataType.LEFT ) );

            DoubleScoreStatistic three =
                    DoubleScoreStatistic.newBuilder()
                                        .setMetric( DoubleScoreMetric.newBuilder()
                                                                     .setName( MetricName.MEAN_ABSOLUTE_ERROR ) )
                                        .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                     .setValue( 68.0 )
                                                                                     .setName( ComponentName.MAIN ) )
                                        .build();


            DoubleScoreStatisticOuter thirdValue =
                    DoubleScoreStatisticOuter.of( three,
                                                  StatisticMetadata.of( SampleMetadata.of( source, timeWindow, third ),
                                                                        1000,
                                                                        MeasurementUnit.of(),
                                                                        MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                                        MetricConstants.MAIN ) );

            statistics.add( thirdValue );

        }

        return Collections.unmodifiableList( statistics );
    }

    /**
     * Do not construct.
     */

    private DataModelTestDataFactory()
    {
    }

}
