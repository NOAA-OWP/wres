package wres.io.concurrency;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.config.specification.MetricSpecification;
import wres.io.config.specification.ScriptFactory;
import wres.io.grouping.LabeledScript;
import wres.io.grouping.LeadResult;
import wres.io.utilities.Database;
import wres.io.utilities.Debug;
import wres.util.NullPrintStream;
import wres.util.ProgressMonitor;
import wres.util.Strings;

/**
 * @author Christopher Tubbs
 *
 */
public class MetricExecutor extends WRESThread implements Callable<List<LeadResult>>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricExecutor.class);
    /**
     * 
     */
    public MetricExecutor(MetricSpecification specification)
    {
        this.specification = specification;
    }

    @Override
    public List<LeadResult> call() throws Exception
    {
        List<LeadResult> results = new ArrayList<>();
        this.executeOnRun();
        
        if (Metrics.hasFunction(this.specification.getMetricType()))
        {
            Map<Integer, Future<Double>> mappedPairs = new TreeMap<>();

            LabeledScript lastLeadScript = ScriptFactory.generateFindLastLead(specification.getSecondVariableID());

            LOGGER.trace("call - about to call Database.getResult() with {} and {}", 
                         lastLeadScript.getScript(), 
                         lastLeadScript.getLabel());

            Integer finalLead = Database.getResult(lastLeadScript.getScript(), lastLeadScript.getLabel());

            LOGGER.trace("call - finished Database.getResult");

            int step = 1;
            
            while (specification.getAggregationSpecification().leadIsValid(step, finalLead))
            {
                MetricStepExecutor stepExecutor = new MetricStepExecutor(this.specification, step);
                stepExecutor.setOnRun(ProgressMonitor.onThreadStartHandler());
                stepExecutor.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                Future<Double> task = Executor.submit(stepExecutor);
                mappedPairs.put(step, task);
                step++;
            }
            
            for (Entry<Integer, Future<Double>>  entry : mappedPairs.entrySet())
            {
                results.add(new LeadResult(entry.getKey(), entry.getValue().get()));
            }
        }

        LOGGER.trace("Results count: {}", results.size());

        this.exectureOnComplete();
        return results;
    }

    private final MetricSpecification specification;
}
