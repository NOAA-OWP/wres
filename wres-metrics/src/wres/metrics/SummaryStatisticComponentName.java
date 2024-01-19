package wres.metrics;

/**
 * A component name for a summary statistic.
 * @author James Brown
 */
public enum SummaryStatisticComponentName
{
    /** The metric to which the diagram refers. Must be a name from {@link wres.config.MetricConstants}. */
    METRIC_NAME,
    /** The Metric component name. Must be a name from {@link wres.config.MetricConstants}. */
    METRIC_COMPONENT_NAME,
    /** The unit of the metric. */
    METRIC_UNIT
}
