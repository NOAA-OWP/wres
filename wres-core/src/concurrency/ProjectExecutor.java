package concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.MainFunctions;
import wres.io.concurrency.MetricTask;
import wres.io.config.ProjectSettings;
import wres.io.config.SystemSettings;
import wres.io.config.specification.MetricSpecification;
import wres.io.config.specification.ProjectDataSpecification;
import wres.io.config.specification.ProjectSpecification;
import wres.io.grouping.LeadResult;
import wres.io.reading.ConfiguredLoader;
import wres.io.utilities.Database;
import wres.util.ProgressMonitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Created by ctubbs on 6/30/17.
 */
public final class ProjectExecutor implements Function<String[], Integer>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProjectExecutor.class);

    // A secondary executor for second-level tasks, should help
    // avoid the situation where task A is waiting for another
    // task B in the queue that won't be executed until
    // tasks in the executor (task A) complete (possible deadlock?)
    private final ExecutorService secondLevelExecutor = Executors.newFixedThreadPool(getNestedThreadCount());

    private ProjectSpecification project;

    private String metricName = null;

    private static int getNestedThreadCount()
    {
        int maxThreadCount = SystemSettings.maximumThreadCount() / 2;
        if (maxThreadCount == 0)
        {
            maxThreadCount = 1;
        }
        return maxThreadCount;
    }

    private void complete()
    {
        if (this.secondLevelExecutor != null)
        {
            this.secondLevelExecutor.shutdownNow();
        }
    }

    private void ingestData()
    {
        final List<Future> fileIngestTasks = new ArrayList<>();

        try
        {
            for (ProjectDataSpecification datasource : project.getDatasources())
            {
                LOGGER.info("Loading datasource information if it doesn't already exist...");
                final ConfiguredLoader dataLoader = new ConfiguredLoader(datasource);
                fileIngestTasks.addAll(dataLoader.load());
                if (LOGGER.isInfoEnabled())
                {
                    LOGGER.info("Queued " + fileIngestTasks.size()
                                        + " file(s) for ingest");
                }
            }
        }
        catch (IOException ioe)
        {
            LOGGER.error("When trying to ingest files:", ioe);
            return;
        }

        try
        {
            for (Future t : fileIngestTasks)
            {
                if (t != null)
                {
                    t.get();
                }
                else
                {
                    LOGGER.debug("Received a null object from ConfiguredLoader");
                }
            }

            if (fileIngestTasks.size() > 0)
            {
                Database.refreshStatistics();
            }
        }
        catch (InterruptedException ie)
        {
            LOGGER.warn("Interrupted during ingest", ie);
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException ee)
        {
            LOGGER.error("Execution failed", ee);
            return;
        }
        ProgressMonitor.resetMonitor();

        LOGGER.info("The data from this dataset has been ingested into the database");

        LOGGER.info("All data specified for this project should now be loaded.");
    }

    private void outputResults(Map<String, List<LeadResult>> results)
    {

        if (results == null || results.size() == 0)
        {
            return;
        }

        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info("");
            LOGGER.info("Project: {}", project.getName());
            LOGGER.info("");

            for (Map.Entry<String, List<LeadResult>> entry : results.entrySet())
            {
                LOGGER.info("");
                LOGGER.info(entry.getKey());
                LOGGER.info("--------------------------------------------------------------------------------------");

                for (LeadResult metricResult : entry.getValue())
                {
                    LOGGER.info(metricResult.getLead() + "\t\t|\t" + metricResult.getResult());
                }

                LOGGER.info("");
            }
        }
    }

    private Map<String, List<LeadResult>> collectResults(Map<String, Future<List<LeadResult>>> futureResults)
    {
        if (futureResults == null || futureResults.size() == 0)
        {
            return null;
        }

        Map<String, List<LeadResult>> results = new TreeMap<>();
        for (Map.Entry<String, Future<List<LeadResult>>> entry : futureResults.entrySet())
        {
            try
            {
                results.put(entry.getKey(), entry.getValue().get());
            }
            catch(InterruptedException ie)
            {
                LOGGER.warn("Interrupted", ie);
                secondLevelExecutor.shutdown();
                Thread.currentThread().interrupt();
            }
            catch (ExecutionException e)
            {
                LOGGER.error("Execution failed", e);
                secondLevelExecutor.shutdown();
            }
        }

        return results;
    }

    private Map<String, Future<List<LeadResult>>> calculateMetrics()
    {
        Map<String, Future<List<LeadResult>>> futureResults = new TreeMap<>();

        if (this.metricName == null)
        {
            for (int metricIndex = 0; metricIndex < this.project.metricCount(); ++metricIndex)
            {
                futureResults.putAll(performMetric(this.project.getMetric(metricIndex)));
            }
        }
        else
        {
            futureResults.putAll(performMetric(project.getMetric(this.metricName)));
        }

        return futureResults;
    }

    private Map<String, Future<List<LeadResult>>> performMetric(MetricSpecification specification)
    {
        Map<String, Future<List<LeadResult>>> futureResults = new TreeMap<>();

        if (specification != null)
        {
            final MetricTask metric = new MetricTask(specification, secondLevelExecutor);
            metric.setOnRun(ProgressMonitor.onThreadStartHandler());
            metric.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
            if (LOGGER.isInfoEnabled())
            {
                LOGGER.info("Now executing the metric named: " + specification.getName());
            }
            futureResults.put(specification.getName(), wres.io.concurrency.Executor.submit(metric));
        }

        return futureResults;
    }

    @Override
    public Integer apply (final String[] args) {
        Integer result = MainFunctions.FAILURE;

        try {
            if (args.length > 0) {
                this.project = ProjectSettings.getProject(args[0]);

                if (args.length > 1) {
                    this.metricName = args[1];
                }

                this.ingestData();

                final Map<String, Future<List<LeadResult>>> futureResults = calculateMetrics();

                final Map<String, List<LeadResult>> results = collectResults(futureResults);

                this.outputResults(results);

                result = MainFunctions.SUCCESS;
            }
            else {
                LOGGER.error("There are not enough arguments to run 'executeProject'");
                LOGGER.error("usage: executeProject <project name> [<metric name>]");
            }
        }
        finally
        {
            this.complete();
        }

        return result;
    }
}
