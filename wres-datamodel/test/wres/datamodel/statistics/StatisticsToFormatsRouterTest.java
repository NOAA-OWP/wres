package wres.datamodel.statistics;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.MetricConstants;
import wres.config.components.Format;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.PairsMetric;
import wres.statistics.generated.PairsStatistic;
import wres.statistics.generated.Statistics;

/**
 * Tests the {@link StatisticsToFormatsRouter}.
 *
 * @author James Brown
 */
class StatisticsToFormatsRouterTest
{
    @Test
    void testApply()
    {
        Map<Format, MetricConstants.StatisticType> actual = new EnumMap<>( Format.class );
        StatisticsToFormatsRouter.Builder builder = new StatisticsToFormatsRouter.Builder();
        builder.addDoubleScoreConsumer( Format.PNG, a -> {
            actual.put( Format.PNG, MetricConstants.StatisticType.DOUBLE_SCORE );
            return Set.of();
        } );
        builder.addDurationScoreConsumer( Format.NETCDF2, a -> {
            actual.put( Format.NETCDF2, MetricConstants.StatisticType.DURATION_SCORE );
            return Set.of();
        } );
        builder.addDiagramConsumer( Format.SVG, a -> {
            actual.put( Format.SVG, MetricConstants.StatisticType.DIAGRAM );
            return Set.of();
        } );
        builder.addDurationDiagramConsumer( Format.PROTOBUF, a -> {
            actual.put( Format.PROTOBUF, MetricConstants.StatisticType.DURATION_DIAGRAM );
            return Set.of();
        } );
        builder.addBoxplotConsumerPerPair( Format.CSV, a -> {
            actual.put( Format.CSV, MetricConstants.StatisticType.BOXPLOT_PER_PAIR );
            return Set.of();
        } );
        builder.addBoxplotConsumerPerPool( Format.GRAPHIC, a -> {
            actual.put( Format.GRAPHIC, MetricConstants.StatisticType.BOXPLOT_PER_POOL );
            return Set.of();
        } );
        builder.addPairsStatisticsConsumer( Format.CSV2, a -> {
            actual.put( Format.CSV2, MetricConstants.StatisticType.PAIRS );
            return Set.of();
        } );
        builder.addStatisticsConsumer( Format.NETCDF, a -> {
            actual.put( Format.NETCDF, MetricConstants.StatisticType.DOUBLE_SCORE );
            return Set.of();
        } );

        builder.setEvaluationDescription( Evaluation.newBuilder()
                                                    .setMeasurementUnit( "foo" )
                                                    .build() );

        StatisticsToFormatsRouter router = builder.build();

        DurationDiagramStatistic durationDiagram =
                DurationDiagramStatistic.newBuilder()
                                        .setMetric( DurationDiagramMetric.newBuilder()
                                                                         .setName( MetricName.TIME_TO_PEAK_ERROR )
                                                                         .build() )
                                        .build();
        DurationScoreStatistic durationScore =
                DurationScoreStatistic.newBuilder()
                                      .setMetric( DurationScoreMetric.newBuilder()
                                                                     .setName( MetricName.TIME_TO_PEAK_ERROR ) )
                                      .build();

        DoubleScoreStatistic doubleScore = DoubleScoreStatistic.newBuilder()
                                                               .setMetric( DoubleScoreMetric.newBuilder()
                                                                                            .setName( MetricName.MEAN_ERROR )
                                                                                            .build() )
                                                               .build();

        DiagramStatistic diagram = DiagramStatistic.newBuilder()
                                                   .setMetric( DiagramMetric.newBuilder()
                                                                            .setName( MetricName.RELIABILITY_DIAGRAM )
                                                                            .build() )
                                                   .build();

        BoxplotStatistic boxPlot = BoxplotStatistic.newBuilder()
                                                   .setMetric( BoxplotMetric.newBuilder()
                                                                            .setName( MetricName.BOX_PLOT_OF_ERRORS )
                                                                            .build() )
                                                   .build();

        PairsStatistic pairsStatistic = PairsStatistic.newBuilder()
                                                      .setMetric( PairsMetric.newBuilder()
                                                                             .setName( MetricName.TIME_SERIES_PLOT )
                                                                             .build() )
                                                      .build();

        Statistics statistics = Statistics.newBuilder()
                                          .addOneBoxPerPool( boxPlot )
                                          .addOneBoxPerPair( boxPlot )
                                          .addDiagrams( diagram )
                                          .addScores( doubleScore )
                                          .addDurationDiagrams( durationDiagram )
                                          .addDurationScores( durationScore )
                                          .addPairsStatistics( pairsStatistic )
                                          .build();

        router.apply( List.of( statistics ) );

        Map<Format, MetricConstants.StatisticType> expected =
                Map.of( Format.PNG, MetricConstants.StatisticType.DOUBLE_SCORE,
                        Format.NETCDF2, MetricConstants.StatisticType.DURATION_SCORE,
                        Format.PROTOBUF, MetricConstants.StatisticType.DURATION_DIAGRAM,
                        Format.SVG, MetricConstants.StatisticType.DIAGRAM,
                        Format.CSV, MetricConstants.StatisticType.BOXPLOT_PER_PAIR,
                        Format.CSV2, MetricConstants.StatisticType.PAIRS,
                        Format.GRAPHIC, MetricConstants.StatisticType.BOXPLOT_PER_POOL,
                        Format.NETCDF, MetricConstants.StatisticType.DOUBLE_SCORE );

        assertEquals( expected, actual );
    }
}
