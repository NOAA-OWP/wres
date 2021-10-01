package wres.pipeline.statistics;

import java.util.ArrayList;
import java.util.List;

import wres.config.generated.DataSourceBaselineConfig;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DateCondition;
import wres.config.generated.DurationUnit;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.SummaryStatisticsConfig;
import wres.config.generated.SummaryStatisticsName;
import wres.config.generated.ThresholdDataType;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.config.generated.TimeSeriesMetricConfig;
import wres.config.generated.TimeSeriesMetricConfigName;
import wres.config.generated.DataSourceConfig.Variable;

/**
 * Generates declarations for integration testing.
 * @author James Brown
 */

class TestDeclarationGenerator
{

    /**
     * @return a declaration for single-valued forecasts with all valid metrics.
     */

    static ProjectConfig getDeclarationForSingleValuedForecastsWithAllValidMetricsAndIssuedDatePools()
    {
        DateCondition issuedDatesConfig = new DateCondition( "2551-03-17T00:00:00Z", "2551-03-20T00:00:00Z" );
        PoolingWindowConfig issuedDatesPoolingWindowConfig =
                new PoolingWindowConfig( 13, 7, DurationUnit.HOURS );
        PairConfig pairsConfig = new PairConfig( "CFS",
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesPoolingWindowConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 null );

        DataSourceConfig left = new DataSourceConfig( DatasourceType.fromValue( "observations" ),
                                                      List.of(),
                                                      new Variable( "DISCHARGE", null ),
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null );

        DataSourceConfig right = new DataSourceConfig( DatasourceType.fromValue( "single valued forecasts" ),
                                                       List.of(),
                                                       new Variable( "STREAMFLOW", null ),
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null );

        ProjectConfig.Inputs inputsConfig = new ProjectConfig.Inputs( left,
                                                                      right,
                                                                      null );

        // Add some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.ALL_VALID ) );

        // Add some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              ThresholdDataType.LEFT,
                                              "0.5",
                                              ThresholdOperator.GREATER_THAN ) );

        MetricsConfig metricsConfig = new MetricsConfig( thresholds, 0, metrics, null );

        return new ProjectConfig( inputsConfig, pairsConfig, List.of( metricsConfig ), null, null, null );
    }

    /**
     * @return a declaration for single-valued forecasts with all valid metrics and issued date pools.
     */

    static ProjectConfig getDeclarationForEnsembleForecastsWithAllValidMetricsAndIssuedDatePools()
    {
        ProjectConfig projectConfig =
                TestDeclarationGenerator.getDeclarationForSingleValuedForecastsWithAllValidMetricsAndIssuedDatePools();

        DataSourceConfig right = new DataSourceConfig( DatasourceType.fromValue( "ensemble forecasts" ),
                                                       List.of(),
                                                       new Variable( "STREAMFLOW", null ),
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null );

        DataSourceBaselineConfig baseline = new DataSourceBaselineConfig( null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null );


        ProjectConfig.Inputs inputsConfig = new ProjectConfig.Inputs( projectConfig.getInputs().getLeft(),
                                                                      right,
                                                                      baseline );

        return new ProjectConfig( inputsConfig, projectConfig.getPair(), projectConfig.getMetrics(), null, null, null );
    }

    /**
     * @return a declaration for single-valued forecasts without thresholds and with nominated metrics.
     */

    static ProjectConfig getDeclarationForSingleValuedForecastsWithoutThresholds()
    {
        PairConfig pairsConfig = new PairConfig( "CFS",
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null );

        DataSourceConfig left = new DataSourceConfig( DatasourceType.fromValue( "observations" ),
                                                      List.of(),
                                                      new Variable( "DISCHARGE", null ),
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null );

        DataSourceConfig right = new DataSourceConfig( DatasourceType.fromValue( "single valued forecasts" ),
                                                       List.of(),
                                                       new Variable( "STREAMFLOW", null ),
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null );

        ProjectConfig.Inputs inputsConfig = new ProjectConfig.Inputs( left,
                                                                      right,
                                                                      null );

        // Add some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_ABSOLUTE_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, MetricConfigName.COEFFICIENT_OF_DETERMINATION ) );
        metrics.add( new MetricConfig( null, MetricConfigName.ROOT_MEAN_SQUARE_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.VOLUMETRIC_EFFICIENCY ) );
        metrics.add( new MetricConfig( null, MetricConfigName.BOX_PLOT_OF_ERRORS ) );

        MetricsConfig metricsConfig = new MetricsConfig( null, 0, metrics, null );

        return new ProjectConfig( inputsConfig, pairsConfig, List.of( metricsConfig ), null, null, null );
    }

    /**
     * @return a declaration for single-valued forecasts with thresholds and with nominated metrics.
     */

    static ProjectConfig getDeclarationForSingleValuedForecastsWithThresholds()
    {
        PairConfig pairsConfig = new PairConfig( "CMS",
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null );

        DataSourceConfig left = new DataSourceConfig( DatasourceType.fromValue( "observations" ),
                                                      List.of(),
                                                      new Variable( "QINE", null ),
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null );

        DataSourceConfig right = new DataSourceConfig( DatasourceType.fromValue( "single valued forecasts" ),
                                                       List.of(),
                                                       new Variable( "SQIN", null ),
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null );

        ProjectConfig.Inputs inputsConfig = new ProjectConfig.Inputs( left,
                                                                      right,
                                                                      null );

        // Add some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_ABSOLUTE_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_SQUARE_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, MetricConfigName.COEFFICIENT_OF_DETERMINATION ) );
        metrics.add( new MetricConfig( null, MetricConfigName.ROOT_MEAN_SQUARE_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.THREAT_SCORE ) );
        metrics.add( new MetricConfig( null, MetricConfigName.CONTINGENCY_TABLE ) );
        metrics.add( new MetricConfig( null, MetricConfigName.QUANTILE_QUANTILE_DIAGRAM ) );
        metrics.add( new MetricConfig( null, MetricConfigName.SAMPLE_SIZE ) );

        // Add some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              ThresholdDataType.LEFT,
                                              "4.9",
                                              ThresholdOperator.GREATER_THAN ) );

        MetricsConfig metricsConfig = new MetricsConfig( thresholds, 0, metrics, null );

        return new ProjectConfig( inputsConfig, pairsConfig, List.of( metricsConfig ), null, null, null );
    }

    /**
     * @return a declaration for single-valued forecasts with all valid metrics.
     */

    static ProjectConfig getDeclarationForSingleValuedForecastsWithAllValidMetrics()
    {
        PairConfig pairsConfig = new PairConfig( "CFS",
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null );

        DataSourceConfig left = new DataSourceConfig( DatasourceType.fromValue( "observations" ),
                                                      List.of(),
                                                      new Variable( "DISCHARGE", null ),
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null );

        DataSourceConfig right = new DataSourceConfig( DatasourceType.fromValue( "single valued forecasts" ),
                                                       List.of(),
                                                       new Variable( "STREAMFLOW", null ),
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null );

        ProjectConfig.Inputs inputsConfig = new ProjectConfig.Inputs( left,
                                                                      right,
                                                                      null );

        // Add some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.ALL_VALID ) );

        // Add some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              ThresholdDataType.LEFT,
                                              "0.5",
                                              ThresholdOperator.GREATER_THAN ) );

        MetricsConfig metricsConfig = new MetricsConfig( thresholds, 0, metrics, null );

        return new ProjectConfig( inputsConfig, pairsConfig, List.of( metricsConfig ), null, null, null );
    }

    /**
     * @return a declaration for single-valued forecasts with all valid metrics.
     */

    static ProjectConfig getDeclarationForSingleValuedForecastsWithTimeSeriesSummaryStatistics()
    {
        PairConfig pairsConfig = new PairConfig( "CFS",
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null );

        DataSourceConfig left = new DataSourceConfig( DatasourceType.fromValue( "observations" ),
                                                      List.of(),
                                                      new Variable( "DISCHARGE", null ),
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null );

        DataSourceConfig right = new DataSourceConfig( DatasourceType.fromValue( "single valued forecasts" ),
                                                       List.of(),
                                                       new Variable( "STREAMFLOW", null ),
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null );

        ProjectConfig.Inputs inputsConfig = new ProjectConfig.Inputs( left,
                                                                      right,
                                                                      null );

        // Add some metrics
        List<TimeSeriesMetricConfig> metrics = new ArrayList<>();
        metrics.add( new TimeSeriesMetricConfig( null,
                                                 TimeSeriesMetricConfigName.TIME_TO_PEAK_ERROR,
                                                 new SummaryStatisticsConfig( List.of( SummaryStatisticsName.MEAN,
                                                                                       SummaryStatisticsName.MEDIAN,
                                                                                       SummaryStatisticsName.MINIMUM,
                                                                                       SummaryStatisticsName.MAXIMUM,
                                                                                       SummaryStatisticsName.MEAN_ABSOLUTE ) ) ) );

        MetricsConfig metricsConfig = new MetricsConfig( null, 0, null, metrics );

        return new ProjectConfig( inputsConfig, pairsConfig, List.of( metricsConfig ), null, null, null );
    }

    /**
     * @return a declaration for ensemble forecasts with all valid metrics and value thresholds.
     */

    static ProjectConfig getDeclarationForEnsembleForecastsWithAllValidMetricsAndValueThresholds()
    {
        ProjectConfig projectConfig =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndIssuedDatePools();

        // Add some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              ThresholdDataType.LEFT,
                                              "0,5,10,15,20",
                                              ThresholdOperator.GREATER_THAN ) );

        MetricsConfig metricsConfig =
                new MetricsConfig( thresholds, 0, projectConfig.getMetrics().get( 0 ).getMetric(), null );

        return new ProjectConfig( projectConfig.getInputs(),
                                  projectConfig.getPair(),
                                  List.of( metricsConfig ),
                                  null,
                                  null,
                                  null );
    }

    /**
     * @return a declaration for ensemble forecasts without thresholds.
     */

    static ProjectConfig getDeclarationForEnsembleForecastsWithoutThresholds()
    {
        ProjectConfig projectConfig =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndIssuedDatePools();

        // Add some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_ABSOLUTE_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, MetricConfigName.COEFFICIENT_OF_DETERMINATION ) );
        metrics.add( new MetricConfig( null, MetricConfigName.ROOT_MEAN_SQUARE_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.CONTINUOUS_RANKED_PROBABILITY_SCORE ) );

        MetricsConfig metricsConfig = new MetricsConfig( null, 0, metrics, null );

        return new ProjectConfig( projectConfig.getInputs(),
                                  projectConfig.getPair(),
                                  List.of( metricsConfig ),
                                  null,
                                  null,
                                  null );
    }

    /**
     * @return a declaration for ensemble forecasts with probability thresholds.
     */

    static ProjectConfig getDeclarationForEnsembleForecastsWithProbabilityThresholds()
    {
        ProjectConfig projectConfig =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndIssuedDatePools();

        // Add some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              ThresholdDataType.LEFT,
                                              "0.0,0.3919,0.4415,0.5042,0.5525",
                                              ThresholdOperator.GREATER_THAN_OR_EQUAL_TO ) );

        // Add some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, MetricConfigName.MEAN_ABSOLUTE_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, MetricConfigName.COEFFICIENT_OF_DETERMINATION ) );
        metrics.add( new MetricConfig( null, MetricConfigName.ROOT_MEAN_SQUARE_ERROR ) );
        metrics.add( new MetricConfig( null, MetricConfigName.BRIER_SKILL_SCORE ) );
        metrics.add( new MetricConfig( null, MetricConfigName.CONTINUOUS_RANKED_PROBABILITY_SCORE ) );
        metrics.add( new MetricConfig( null, MetricConfigName.RANK_HISTOGRAM ) );
        metrics.add( new MetricConfig( null, MetricConfigName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM ) );
        metrics.add( new MetricConfig( null, MetricConfigName.SAMPLE_SIZE ) );

        MetricsConfig metricsConfig = new MetricsConfig( thresholds, 0, metrics, null );

        return new ProjectConfig( projectConfig.getInputs(),
                                  projectConfig.getPair(),
                                  List.of( metricsConfig ),
                                  null,
                                  null,
                                  null );
    }

    /**
     * @return a declaration for single-valued forecasts with all valid metrics.
     */

    static ProjectConfig getDeclarationForEnsembleForecastsWithAllValidMetrics()
    {
        ProjectConfig projectConfig =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndIssuedDatePools();

        PairConfig pairsConfig = new PairConfig( "CFS",
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null );

        return new ProjectConfig( projectConfig.getInputs(),
                                  pairsConfig,
                                  projectConfig.getMetrics(),
                                  null,
                                  null,
                                  null );
    }

    /**
     * @return a declaration for ensemble forecasts with a contingency table and value thresholds.
     */

    static ProjectConfig getDeclarationForEnsembleForecastsWithContingencyTableAndValueThresholds()
    {
        ProjectConfig projectConfig =
                TestDeclarationGenerator.getDeclarationForEnsembleForecastsWithAllValidMetricsAndIssuedDatePools();

        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null );

        // Add some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              ThresholdDataType.LEFT,
                                              "50.0",
                                              ThresholdOperator.GREATER_THAN ) );
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY_CLASSIFIER,
                                              ThresholdDataType.LEFT,
                                              "0.05, 0.25, 0.5, 0.75, 0.9, 0.95",
                                              ThresholdOperator.GREATER_THAN ) );
        // Add some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.CONTINGENCY_TABLE ) );

        MetricsConfig metricsConfig = new MetricsConfig( thresholds, 0, metrics, null );

        return new ProjectConfig( projectConfig.getInputs(),
                                  pairsConfig,
                                  List.of( metricsConfig ),
                                  null,
                                  null,
                                  null );
    }

    /**
     * Do not construct.
     */

    private TestDeclarationGenerator()
    {
    }
}
