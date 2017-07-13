package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.DiscreteProbabilityPairs;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputFactory;
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
     * Instance of an {@link MetricOutputFactory} for building metric outputs.
     */

    private MetricOutputFactory outputFactory = null;

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
     * @param outputFactory a {@link MetricOutputFactory}
     * @return a {@link MetricFactory}
     */

    public static MetricFactory getInstance(final MetricOutputFactory outputFactory)
    {
        //Lazy construction
        if(Objects.isNull(instance))
        {
            instance = new MetricFactory(outputFactory);
        }
        return instance;
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
            switch(next)
            {
                case MEAN_ABSOLUTE_ERROR:
                    builder.add(ofMeanAbsoluteError());
                    break;
                case MEAN_ERROR:
                    builder.add(ofMeanError());
                    break;
                case ROOT_MEAN_SQUARE_ERROR:
                    builder.add(ofRootMeanSquareError());
                    break;
                default:
                    throw new IllegalArgumentException(error +" '"+ next + "'.");
            }
        }
        builder.setOutputFactory(outputFactory);
        return builder.build();
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
            switch(next)
            {
                case MEAN_SQUARE_ERROR:
                    builder.add(ofMeanSquareError());
                    break;
                case MEAN_SQUARE_ERROR_SKILL_SCORE:
                    builder.add(ofMeanSquareErrorSkillScore());
                    break;
                default:
                    throw new IllegalArgumentException(error +" '"+ next + "'.");
            }
        }
        builder.setOutputFactory(outputFactory);
        return builder.build();
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
            switch(next)
            {
                case BRIER_SCORE:
                    builder.add(ofBrierScore());
                    break;
                case BRIER_SKILL_SCORE:
                    builder.add(ofBrierSkillScore());
                    break;
                default:
                    throw new IllegalArgumentException(error +" '"+ next + "'.");
            }
        }
        builder.setOutputFactory(outputFactory);
        return builder.build();
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
            switch(next)
            {
                case CRITICAL_SUCCESS_INDEX:
                    builder.add(ofCriticalSuccessIndex());
                    break;
                case EQUITABLE_THREAT_SCORE:
                    builder.add(ofEquitableThreatScore());
                    break;
                case PEIRCE_SKILL_SCORE:
                    builder.add(ofPeirceSkillScore());
                    break;    
                case PROBABILITY_OF_DETECTION:
                    builder.add(ofProbabilityOfDetection());
                    break;    
                case PROBABILITY_OF_FALSE_DETECTION:
                    builder.add(ofProbabilityOfFalseDetection());
                    break;     
                default:
                    throw new IllegalArgumentException(error +" '"+ next + "'.");
            }
        }
        builder.setOutputFactory(outputFactory);
        return builder.build();
    }

    /**
     * Return a default {@link BrierScore} function.
     * 
     * @return a default {@link BrierScore} function.
     */

    public BrierScore ofBrierScore()
    {
        return (BrierScore)new BrierScore.BrierScoreBuilder().setOutputFactory(outputFactory).build();
    }

    /**
     * Return a {@link BrierScore} function with a prescribed decomposition. See {@link Score#getDecompositionID()}.
     * 
     * @param decompositionID the decompositionID
     * @return a {@link BrierScore} function.
     */

    public BrierScore ofBrierScore(final MetricConstants decompositionID)
    {
        return (BrierScore)new BrierScore.BrierScoreBuilder().setDecompositionID(decompositionID)
                                                             .setOutputFactory(outputFactory)
                                                             .build();
    }

    /**
     * Return a default {@link BrierSkillScore} function.
     * 
     * @return a default {@link BrierSkillScore} function.
     */

    public BrierSkillScore ofBrierSkillScore()
    {
        return (BrierSkillScore)new BrierSkillScore.BrierSkillScoreBuilder().setOutputFactory(outputFactory).build();
    }

    /**
     * Return a default {@link ContingencyTable} function.
     * 
     * @return a default {@link ContingencyTable} function.
     */

    public ContingencyTable<MulticategoryPairs> ofContingencyTable()
    {
        return (ContingencyTable<MulticategoryPairs>)new ContingencyTable.ContingencyTableBuilder<>().setOutputFactory(outputFactory)
                                                                                                     .build();
    }

    /**
     * Return a default {@link CriticalSuccessIndex} function.
     * 
     * @return a default {@link CriticalSuccessIndex} function.
     */

    public CriticalSuccessIndex ofCriticalSuccessIndex()
    {
        return (CriticalSuccessIndex)new CriticalSuccessIndex.CriticalSuccessIndexBuilder().setOutputFactory(outputFactory)
                                                                                           .build();
    }

    /**
     * Return a default {@link EquitableThreatScore} function.
     * 
     * @return a default {@link EquitableThreatScore} function.
     */

    public EquitableThreatScore ofEquitableThreatScore()
    {
        return (EquitableThreatScore)new EquitableThreatScore.EquitableThreatScoreBuilder().setOutputFactory(outputFactory)
                                                                                           .build();
    }

    /**
     * Return a default {@link MeanAbsoluteError} function.
     * 
     * @return a default {@link MeanAbsoluteError} function.
     */

    public MeanAbsoluteError ofMeanAbsoluteError()
    {
        return (MeanAbsoluteError)new MeanAbsoluteError.MeanAbsoluteErrorBuilder().setOutputFactory(outputFactory)
                                                                                  .build();
    }

    /**
     * Return a default {@link MeanError} function.
     * 
     * @return a default {@link MeanError} function.
     */

    public MeanError ofMeanError()
    {
        return (MeanError)new MeanError.MeanErrorBuilder().setOutputFactory(outputFactory).build();
    }

    /**
     * Return a default {@link MeanSquareError} function.
     * 
     * @return a default {@link MeanSquareError} function.
     */

    public MeanSquareError<SingleValuedPairs> ofMeanSquareError()
    {
        return (MeanSquareError<SingleValuedPairs>)new MeanSquareError.MeanSquareErrorBuilder<>().setOutputFactory(outputFactory)
                                                                                                 .build();
    }

    /**
     * Return a default {@link MeanSquareErrorSkillScore} function.
     * 
     * @return a default {@link MeanSquareErrorSkillScore} function.
     */

    public MeanSquareErrorSkillScore<SingleValuedPairs> ofMeanSquareErrorSkillScore()
    {
        return (MeanSquareErrorSkillScore<SingleValuedPairs>)new MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder<>().setOutputFactory(outputFactory)
                                                                                                                               .build();
    }

    /**
     * Return a default {@link PeirceSkillScore} function for a dichotomous event.
     * 
     * @return a default {@link PeirceSkillScore} function for a dichotomous event.
     */

    public PeirceSkillScore<DichotomousPairs> ofPeirceSkillScore()
    {
        return (PeirceSkillScore<DichotomousPairs>)new PeirceSkillScore.PeirceSkillScoreBuilder<DichotomousPairs>().setOutputFactory(outputFactory)
                                                                                                                   .build();
    }

    /**
     * Return a default {@link PeirceSkillScore} function for a multicategory event.
     * 
     * @return a default {@link PeirceSkillScore} function for a multicategory event.
     */

    public PeirceSkillScore<MulticategoryPairs> ofPeirceSkillScoreMulti()
    {
        return (PeirceSkillScore<MulticategoryPairs>)new PeirceSkillScore.PeirceSkillScoreBuilder<MulticategoryPairs>().setOutputFactory(outputFactory)
                                                                                                                       .build();
    }

    /**
     * Return a default {@link ProbabilityOfDetection} function.
     * 
     * @return a default {@link ProbabilityOfDetection} function.
     */

    public ProbabilityOfDetection ofProbabilityOfDetection()
    {
        return (ProbabilityOfDetection)new ProbabilityOfDetection.ProbabilityOfDetectionBuilder().setOutputFactory(outputFactory)
                                                                                                 .build();
    }

    /**
     * Return a default {@link ProbabilityOfFalseDetection} function.
     * 
     * @return a default {@link ProbabilityOfFalseDetection} function.
     */

    public ProbabilityOfFalseDetection ofProbabilityOfFalseDetection()
    {
        return (ProbabilityOfFalseDetection)new ProbabilityOfFalseDetection.ProbabilityOfFalseDetectionBuilder().setOutputFactory(outputFactory)
                                                                                                                .build();
    }

    /**
     * Return a default {@link RootMeanSquareError} function.
     * 
     * @return a default {@link RootMeanSquareError} function.
     */

    public RootMeanSquareError ofRootMeanSquareError()
    {
        return (RootMeanSquareError)new RootMeanSquareError.RootMeanSquareErrorBuilder().setOutputFactory(outputFactory)
                                                                                        .build();
    }

    /**
     * Hidden constructor.
     * 
     * @param outputFactory a {@link MetricOutputFactory}
     */

    private MetricFactory(final MetricOutputFactory outputFactory)
    {
        if(Objects.isNull(outputFactory))
        {
            throw new IllegalArgumentException("Specify a non-null metric output factory to construct the "
                + "metric factory.");
        }
        this.outputFactory = outputFactory;
    }

}
