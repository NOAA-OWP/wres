package wres.engine.statistics.metric;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.DiscreteProbabilityPairs;
import wres.datamodel.metric.MatrixOutput;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricConstants.MetricGroup;
import wres.datamodel.metric.MulticategoryPairs;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.VectorOutput;
import wres.engine.statistics.metric.Metric.MetricBuilder;
import wres.engine.statistics.metric.MetricCollection.MetricCollectionBuilder;
import wres.engine.statistics.metric.parameters.MetricParameter;

/**
 * <p>
 * A factory class for constructing metrics.
 * </p>
 * <p>
 * TODO: support construction with parameters by first defining a setParameters(EnumMap mapping) in the
 * {@link MetricBuilder} and then adding parametric methods here. The EnumMap should map an Enum of parameter
 * identifiers to {@link MetricParameter}.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class MetricFactory
{

    /**
     * Instance of an {@link DataFactory} for building metric outputs.
     */

    private DataFactory outputFactory = null;

    /**
     * Instance of the factory.
     */

    private static MetricFactory instance = null;

    /**
     * String used in several error messages.
     */

    private static final String error = "Unrecognized metric for identifier";

    /**
     * Returns an instance of a {@link MetricFactory}.
     * 
     * @param dataFactory a {@link DataFactory}
     * @return a {@link MetricFactory}
     */

    public static MetricFactory getInstance(final DataFactory dataFactory)
    {
        //Lazy construction
        if(Objects.isNull(instance))
        {
            instance = new MetricFactory(dataFactory);
        }
        return instance;
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link ScalarOutput} or null if no such metrics exist within the input {@link ProjectConfig}.
     * 
     * @param config the project configuration
     * @return a collection of metrics or null
     */

    public MetricCollection<SingleValuedPairs, ScalarOutput> ofSingleValuedScalarCollection(ProjectConfig config)
    {
        //Obtain the list of metrics and find the matching metrics 
        MetricConstants[] metrics = getMetricsFromConfig(config, MetricGroup.SINGLE_VALUED_SCALAR);
        return metrics.length == 0 ? null : ofSingleValuedScalarCollection(metrics);
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link ScalarOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<SingleValuedPairs, ScalarOutput> ofSingleValuedScalarCollection(MetricConstants... metric)
    {
        final MetricCollectionBuilder<SingleValuedPairs, ScalarOutput> builder = MetricCollectionBuilder.of();
        for(MetricConstants next: metric)
        {
            builder.add(ofSingleValuedScalar(next));
        }
        builder.setOutputFactory(outputFactory);
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link VectorOutput} or null if no such metrics exist within the input {@link ProjectConfig}.
     * 
     * @param config the project configuration
     * @return a collection of metrics or null
     */

    public MetricCollection<SingleValuedPairs, VectorOutput> ofSingleValuedVectorCollection(ProjectConfig config)
    {
        //Obtain the list of metrics and find the matching metrics 
        MetricConstants[] metrics = getMetricsFromConfig(config, MetricGroup.SINGLE_VALUED_VECTOR);
        return metrics.length == 0 ? null : ofSingleValuedVectorCollection(metrics);
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link VectorOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<SingleValuedPairs, VectorOutput> ofSingleValuedVectorCollection(MetricConstants... metric)
    {
        final MetricCollectionBuilder<SingleValuedPairs, VectorOutput> builder = MetricCollectionBuilder.of();
        for(MetricConstants next: metric)
        {
            builder.add(ofSingleValuedVector(next));
        }
        builder.setOutputFactory(outputFactory);
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DiscreteProbabilityPairs} and produce
     * {@link VectorOutput} or null if no such metrics exist within the input {@link ProjectConfig}.
     * 
     * @param config the project configuration
     * @return a collection of metrics or null
     */

    public MetricCollection<DiscreteProbabilityPairs, VectorOutput> ofDiscreteProbabilityVectorCollection(ProjectConfig config)
    {
        //Obtain the list of metrics and find the matching metrics 
        MetricConstants[] metrics = getMetricsFromConfig(config, MetricGroup.DISCRETE_PROBABILITY_VECTOR);
        return metrics.length == 0 ? null : ofDiscreteProbabilityVectorCollection(metrics);
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DiscreteProbabilityPairs} and produce
     * {@link VectorOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<DiscreteProbabilityPairs, VectorOutput> ofDiscreteProbabilityVectorCollection(MetricConstants... metric)
    {
        final MetricCollectionBuilder<DiscreteProbabilityPairs, VectorOutput> builder = MetricCollectionBuilder.of();
        for(MetricConstants next: metric)
        {
            builder.add(ofDiscreteProbabilityVector(next));
        }
        builder.setOutputFactory(outputFactory);
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DichotomousPairs} and produce
     * {@link ScalarOutput} or null if no such metrics exist within the input {@link ProjectConfig}.
     * 
     * @param config the project configuration
     * @return a collection of metrics or null
     */

    public MetricCollection<DichotomousPairs, ScalarOutput> ofDichotomousScalarCollection(ProjectConfig config)
    {
        //Obtain the list of metrics and find the matching metrics 
        MetricConstants[] metrics = getMetricsFromConfig(config, MetricGroup.DICHOTOMOUS_SCALAR);
        return metrics.length == 0 ? null : ofDichotomousScalarCollection(metrics);
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DichotomousPairs} and produce
     * {@link ScalarOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<DichotomousPairs, ScalarOutput> ofDichotomousScalarCollection(MetricConstants... metric)
    {
        final MetricCollectionBuilder<DichotomousPairs, ScalarOutput> builder = MetricCollectionBuilder.of();
        for(MetricConstants next: metric)
        {
            builder.add(ofDichotomousScalar(next));
        }
        builder.setOutputFactory(outputFactory);
        return builder.build();
    }

    /**
     * Returns a {@link Metric} that consumes {@link SingleValuedPairs} and produces {@link ScalarOutput}.
     * 
     * @param metric the metric identifier
     * @return the metric
     */

    public Metric<SingleValuedPairs, ScalarOutput> ofSingleValuedScalar(MetricConstants metric)
    {
        switch(metric)
        {
            case BIAS_FRACTION:
                return ofBiasFraction();
            case MEAN_ABSOLUTE_ERROR:
                return ofMeanAbsoluteError();
            case MEAN_ERROR:
                return ofMeanError();
            case ROOT_MEAN_SQUARE_ERROR:
                return ofRootMeanSquareError();
            case CORRELATION_PEARSONS:
                return ofCorrelationPearsons();
            case COEFFICIENT_OF_DETERMINATION:
                return ofCoefficientOfDetermination();
            default:
                throw new IllegalArgumentException(error + " '" + metric + "'.");
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link SingleValuedPairs} and produces {@link VectorOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     */

    public Metric<SingleValuedPairs, VectorOutput> ofSingleValuedVector(MetricConstants metric)
    {
        switch(metric)
        {
            case MEAN_SQUARE_ERROR:
                return ofMeanSquareError();
            case MEAN_SQUARE_ERROR_SKILL_SCORE:
                return ofMeanSquareErrorSkillScore();
            default:
                throw new IllegalArgumentException(error + " '" + metric + "'.");
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link DiscreteProbabilityPairs} and produces {@link VectorOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     */

    public Metric<DiscreteProbabilityPairs, VectorOutput> ofDiscreteProbabilityVector(MetricConstants metric)
    {
        switch(metric)
        {
            case BRIER_SCORE:
                return ofBrierScore();
            case BRIER_SKILL_SCORE:
                return ofBrierSkillScore();
            default:
                throw new IllegalArgumentException(error + " '" + metric + "'.");
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link DichotomousPairs} and produces {@link ScalarOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     */

    public Metric<DichotomousPairs, ScalarOutput> ofDichotomousScalar(MetricConstants metric)
    {
        switch(metric)
        {
            case CRITICAL_SUCCESS_INDEX:
                return ofCriticalSuccessIndex();
            case EQUITABLE_THREAT_SCORE:
                return ofEquitableThreatScore();
            case PEIRCE_SKILL_SCORE:
                return ofPeirceSkillScore();
            case PROBABILITY_OF_DETECTION:
                return ofProbabilityOfDetection();
            case PROBABILITY_OF_FALSE_DETECTION:
                return ofProbabilityOfFalseDetection();
            default:
                throw new IllegalArgumentException(error + " '" + metric + "'.");
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link MulticategoryPairs} and produces {@link ScalarOutput}. Use
     * {@link #ofDichotomousScalar(MetricConstants)} when the inputs are dichotomous.
     * 
     * @param metric the metric identifier
     * @return a metric
     */

    public Metric<MulticategoryPairs, ScalarOutput> ofMulticategoryScalar(MetricConstants metric)
    {
        switch(metric)
        {
            case PEIRCE_SKILL_SCORE:
                return ofPeirceSkillScoreMulti();
            default:
                throw new IllegalArgumentException(error + " '" + metric + "'.");
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link MulticategoryPairs} and produces {@link MatrixOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     */

    public Metric<MulticategoryPairs, MatrixOutput> ofMulticategoryMatrix(MetricConstants metric)
    {
        switch(metric)
        {
            case CONTINGENCY_TABLE:
                return ofContingencyTable();
            default:
                throw new IllegalArgumentException(error + " '" + metric + "'.");
        }
    }

    /**
     * Return a default {@link BiasFraction} function.
     * 
     * @return a default {@link BiasFraction} function.
     */

    protected BiasFraction ofBiasFraction()
    {
        return (BiasFraction)new BiasFraction.BiasFractionBuilder().setOutputFactory(outputFactory).build();
    }

    /**
     * Return a default {@link BrierScore} function.
     * 
     * @return a default {@link BrierScore} function.
     */

    protected BrierScore ofBrierScore()
    {
        return (BrierScore)new BrierScore.BrierScoreBuilder().setOutputFactory(outputFactory).build();
    }

    /**
     * Return a default {@link BrierSkillScore} function.
     * 
     * @return a default {@link BrierSkillScore} function.
     */

    protected BrierSkillScore ofBrierSkillScore()
    {
        return (BrierSkillScore)new BrierSkillScore.BrierSkillScoreBuilder().setOutputFactory(outputFactory).build();
    }

    /**
     * Return a default {@link CoefficientOfDetermination} function.
     * 
     * @return a default {@link CoefficientOfDetermination} function.
     */

    protected CorrelationPearsons ofCoefficientOfDetermination()
    {
        return (CoefficientOfDetermination)new CoefficientOfDetermination.CoefficientOfDeterminationBuilder().setOutputFactory(outputFactory)
                                                                                                             .build();
    }

    /**
     * Return a default {@link ContingencyTable} function.
     * 
     * @return a default {@link ContingencyTable} function.
     */

    protected ContingencyTable<MulticategoryPairs> ofContingencyTable()
    {
        return (ContingencyTable<MulticategoryPairs>)new ContingencyTable.ContingencyTableBuilder<>().setOutputFactory(outputFactory)
                                                                                                     .build();
    }

    /**
     * Return a default {@link CorrelationPearsons} function.
     * 
     * @return a default {@link CorrelationPearsons} function.
     */

    protected CorrelationPearsons ofCorrelationPearsons()
    {
        return (CorrelationPearsons)new CorrelationPearsons.CorrelationPearsonsBuilder().setOutputFactory(outputFactory)
                                                                                        .build();
    }

    /**
     * Return a default {@link CriticalSuccessIndex} function.
     * 
     * @return a default {@link CriticalSuccessIndex} function.
     */

    protected CriticalSuccessIndex ofCriticalSuccessIndex()
    {
        return (CriticalSuccessIndex)new CriticalSuccessIndex.CriticalSuccessIndexBuilder().setOutputFactory(outputFactory)
                                                                                           .build();
    }

    /**
     * Return a default {@link EquitableThreatScore} function.
     * 
     * @return a default {@link EquitableThreatScore} function.
     */

    protected EquitableThreatScore ofEquitableThreatScore()
    {
        return (EquitableThreatScore)new EquitableThreatScore.EquitableThreatScoreBuilder().setOutputFactory(outputFactory)
                                                                                           .build();
    }

    /**
     * Return a default {@link MeanAbsoluteError} function.
     * 
     * @return a default {@link MeanAbsoluteError} function.
     */

    protected MeanAbsoluteError ofMeanAbsoluteError()
    {
        return (MeanAbsoluteError)new MeanAbsoluteError.MeanAbsoluteErrorBuilder().setOutputFactory(outputFactory)
                                                                                  .build();
    }

    /**
     * Return a default {@link MeanError} function.
     * 
     * @return a default {@link MeanError} function.
     */

    protected MeanError ofMeanError()
    {
        return (MeanError)new MeanError.MeanErrorBuilder().setOutputFactory(outputFactory).build();
    }

    /**
     * Return a default {@link MeanSquareError} function.
     * 
     * @return a default {@link MeanSquareError} function.
     */

    protected MeanSquareError<SingleValuedPairs> ofMeanSquareError()
    {
        return (MeanSquareError<SingleValuedPairs>)new MeanSquareError.MeanSquareErrorBuilder<>().setOutputFactory(outputFactory)
                                                                                                 .build();
    }

    /**
     * Return a default {@link MeanSquareErrorSkillScore} function.
     * 
     * @return a default {@link MeanSquareErrorSkillScore} function.
     */

    protected MeanSquareErrorSkillScore<SingleValuedPairs> ofMeanSquareErrorSkillScore()
    {
        return (MeanSquareErrorSkillScore<SingleValuedPairs>)new MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder<>().setOutputFactory(outputFactory)
                                                                                                                               .build();
    }

    /**
     * Return a default {@link PeirceSkillScore} function for a dichotomous event.
     * 
     * @return a default {@link PeirceSkillScore} function for a dichotomous event.
     */

    protected PeirceSkillScore<DichotomousPairs> ofPeirceSkillScore()
    {
        return (PeirceSkillScore<DichotomousPairs>)new PeirceSkillScore.PeirceSkillScoreBuilder<DichotomousPairs>().setOutputFactory(outputFactory)
                                                                                                                   .build();
    }

    /**
     * Return a default {@link PeirceSkillScore} function for a multicategory event.
     * 
     * @return a default {@link PeirceSkillScore} function for a multicategory event.
     */

    protected PeirceSkillScore<MulticategoryPairs> ofPeirceSkillScoreMulti()
    {
        return (PeirceSkillScore<MulticategoryPairs>)new PeirceSkillScore.PeirceSkillScoreBuilder<MulticategoryPairs>().setOutputFactory(outputFactory)
                                                                                                                       .build();
    }

    /**
     * Return a default {@link ProbabilityOfDetection} function.
     * 
     * @return a default {@link ProbabilityOfDetection} function.
     */

    protected ProbabilityOfDetection ofProbabilityOfDetection()
    {
        return (ProbabilityOfDetection)new ProbabilityOfDetection.ProbabilityOfDetectionBuilder().setOutputFactory(outputFactory)
                                                                                                 .build();
    }

    /**
     * Return a default {@link ProbabilityOfFalseDetection} function.
     * 
     * @return a default {@link ProbabilityOfFalseDetection} function.
     */

    protected ProbabilityOfFalseDetection ofProbabilityOfFalseDetection()
    {
        return (ProbabilityOfFalseDetection)new ProbabilityOfFalseDetection.ProbabilityOfFalseDetectionBuilder().setOutputFactory(outputFactory)
                                                                                                                .build();
    }

    /**
     * Return a default {@link RootMeanSquareError} function.
     * 
     * @return a default {@link RootMeanSquareError} function.
     */

    protected RootMeanSquareError ofRootMeanSquareError()
    {
        return (RootMeanSquareError)new RootMeanSquareError.RootMeanSquareErrorBuilder().setOutputFactory(outputFactory)
                                                                                        .build();
    }

    /**
     * Returns a set of {@link MetricConstants} from a {@link ProjectConfig} for a specified {@link MetricGroup} or null
     * if no metrics exist.
     * 
     * @param config the project configuration
     * @return a set of {@link MetricConstants} for a specified {@link MetricGroup} or null
     */

    private static MetricConstants[] getMetricsFromConfig(ProjectConfig config, MetricGroup group)
    {
        Objects.requireNonNull(config, "Specify a non-null project from which to generate metrics.");
        //Obtain the list of metrics
        List<MetricConfigName> metricsConfig = config.getOutputs()
                                                     .getMetric()
                                                     .stream()
                                                     .map(MetricConfig::getValue)
                                                     .collect(Collectors.toList());
        //Find the matching metrics 
        Set<MetricConstants> metrics = group.getMetrics();
        metrics.removeIf(a -> !metricsConfig.contains(a.toMetricConfigName()));
        return metrics.toArray(new MetricConstants[metrics.size()]);
    }

    /**
     * Hidden constructor.
     * 
     * @param dataFactory a {@link DataFactory}
     */

    private MetricFactory(final DataFactory dataFactory)
    {
        if(Objects.isNull(dataFactory))
        {
            throw new IllegalArgumentException("Specify a non-null metric output factory to construct the "
                + "metric factory.");
        }
        this.outputFactory = dataFactory;
    }

}
