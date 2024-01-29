package wres.pipeline;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.components.EvaluationDeclaration;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.thresholds.MetricsAndThresholds;
import wres.datamodel.time.TimeSeriesStore;
import wres.events.EvaluationMessager;
import wres.events.subscribe.SubscriberApprover;
import wres.io.database.caching.DatabaseCaches;
import wres.io.project.Project;
import wres.metrics.SummaryStatisticsCalculator;
import wres.system.SystemSettings;

/**
 * Small value class to collect together variables needed to instantiate an evaluation.
 *
 * @param systemSettings the system settings
 * @param declaration the project declaration
 * @param evaluationId the evaluation identifier
 * @param subscriberApprover the subscriber approver
 * @param monitor the evaluation event monitor
 * @param databaseServices the database services
 * @param caches the database caches/ORMs
 * @param metricsAndThresholds the metrics and thresholds
 * @param project the project
 * @param evaluation the evaluation
 * @param timeSeriesStore the time-series data store
 * @param summaryStatistics the summary statistics calculators
 * @param summaryStatisticsForBaseline the summary statistics calculators for baseline datasets
 * @param summaryStatisticsOnly the geometries for which only summary statistics are needed, not raw statistics
 *
 * @author James Brown
 */

@RecordBuilder
record EvaluationDetails( SystemSettings systemSettings,
                          EvaluationDeclaration declaration,
                          String evaluationId,
                          SubscriberApprover subscriberApprover,
                          EvaluationEvent monitor,
                          DatabaseServices databaseServices,
                          DatabaseCaches caches,
                          Set<MetricsAndThresholds> metricsAndThresholds,
                          Project project,
                          EvaluationMessager evaluation,
                          TimeSeriesStore timeSeriesStore,
                          List<SummaryStatisticsCalculator> summaryStatistics,
                          List<SummaryStatisticsCalculator> summaryStatisticsForBaseline,
                          Set<FeatureGroup> summaryStatisticsOnly )
{
    /**
     * @return true if there is an in-memory store of time-series, false otherwise.
     */

    boolean hasInMemoryStore()
    {
        return Objects.nonNull( this.timeSeriesStore );
    }
}
