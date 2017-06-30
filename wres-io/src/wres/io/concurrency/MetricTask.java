package wres.io.concurrency;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.config.specification.MetricSpecification;
import wres.io.config.specification.ScriptFactory;
import wres.io.grouping.LabeledScript;
import wres.io.grouping.LeadResult;
import wres.io.utilities.Database;
import wres.util.ProgressMonitor;

/**
 * @author Christopher Tubbs
 *
 */
public class MetricTask extends WRESTask implements Callable<List<LeadResult>>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricTask.class);

    private final MetricSpecification specification;
    private final ExecutorService secondaryExecutor;

    /**
     * 
     */
    public MetricTask(MetricSpecification specification,
                      ExecutorService secondaryExecutor)
    {
        this.specification = specification;
        this.secondaryExecutor = secondaryExecutor;
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
                ProgressMonitor.increment();
                MetricStepTask stepTask = new MetricStepTask(this.specification, step);
                stepTask.setOnRun(ProgressMonitor.onThreadStartHandler());
                stepTask.setOnComplete(ProgressMonitor.onThreadCompleteHandler());

                Future<Double> task = null;

                if (secondaryExecutor == null)
                {
                    mappedPairs.put(step, Executor.submit(stepTask));
                }
                else
                {
                    mappedPairs.put(step, this.secondaryExecutor.submit(stepTask));
                }

                step++;
            }

            System.out.println(NEWLINE + "All subtasks to calculate " + this.specification.getName() + " have been generated and are now running.");

            for (Entry<Integer, Future<Double>>  entry : mappedPairs.entrySet())
            {
                results.add(new LeadResult(entry.getKey(), entry.getValue().get()));
                ProgressMonitor.completeStep();
                Double result = entry.getValue().get();
                if (result != null)
                {
                    LeadResult lr = new LeadResult(entry.getKey(), result);
                    results.add(lr);
                }
                else
                {
                    LOGGER.debug("null result!");
                }
            }
        }

        LOGGER.debug("Results count: {}", results.size());

        this.executeOnComplete();
        return results;
    }
}
