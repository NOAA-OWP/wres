/**
 * 
 */
package concurrency;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import collections.TwoTuple;
import config.specification.MetricSpecification;
import config.specification.ScriptFactory;
import util.Database;
import util.Utilities;

/**
 * @author Christopher Tubbs
 *
 */
public class MetricExecutor extends WRESThread implements Callable<List<TwoTuple<Integer, Double>>>
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
    public List<TwoTuple<Integer, Double>> call() throws Exception
    {
        List<TwoTuple<Integer, Double>> results = new ArrayList<>();
        this.executeOnRun();
        
        if (Metrics.hasFunction(this.specification.getMetricType()))
        {
            Map<Integer, Future<Double>> mappedPairs = new TreeMap<>();

            TwoTuple<String, String> lastLeadScript = ScriptFactory.generateFindLastLead(specification.getSecondVariableID());

            if (LOGGER.isTraceEnabled())
            {
                LOGGER.trace("call - about to call Database.getResult() with {} and {}",
                             lastLeadScript.getItemOne(), lastLeadScript.getItemTwo());
            }

            Integer finalLead = Database.getResult(lastLeadScript.getItemOne(), lastLeadScript.getItemTwo());

            LOGGER.trace("call - finished Database.getResult");

            int step = 1;
            
            while (specification.getAggregationSpecification().leadIsValid(step, finalLead))
            {
                MetricStepExecutor stepExecutor = new MetricStepExecutor(this.specification, step);
                stepExecutor.setOnRun(Utilities.defaultOnThreadStartHandler());
                stepExecutor.setOnComplete(Utilities.defaultOnThreadCompleteHandler());
                mappedPairs.put(step, Executor.submit(stepExecutor));
                step++;
            }
            
            for (Entry<Integer, Future<Double>>  entry : mappedPairs.entrySet())
            {
                results.add(new TwoTuple(entry.getKey(), entry.getValue().get()));
            }
        }

        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Results count: {}", results.size());
        }
        this.exectureOnComplete();
        return results;
    }

    private final MetricSpecification specification;
}
