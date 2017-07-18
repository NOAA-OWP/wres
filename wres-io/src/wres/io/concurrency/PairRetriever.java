package wres.io.concurrency;

import wres.config.generated.ProjectConfig;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metric.DefaultMetricInputFactory;
import wres.datamodel.metric.MetricInputFactory;
import wres.io.config.specification.ScriptFactory;
import wres.io.utilities.Database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * Created by ctubbs on 7/17/17.
 */
public final class PairRetriever extends WRESTask implements Callable<List<PairOfDoubleAndVectorOfDoubles>> {

    public PairRetriever(ProjectConfig projectConfig, int progress)
    {
        this.projectConfig = projectConfig;
        this.progress = progress;
    }

    @Override
    public List<PairOfDoubleAndVectorOfDoubles> call () throws Exception {
        this.executeOnRun();
        List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();

        Connection connection = null;
        ResultSet resultingPairs = null;

        final MetricInputFactory dataFactory = DefaultMetricInputFactory.getInstance();

        try
        {
            final String script = ScriptFactory.generateGetPairData(this.projectConfig, this.progress);
            connection = Database.getConnection();
            resultingPairs = Database.getResults(connection, script);

            while (resultingPairs.next())
            {
                pairs.add(dataFactory.pairOf(resultingPairs.getDouble("sourceOneValue"),
                                             Stream.of((Double[]) resultingPairs
                                                     .getArray("measurements")
                                                     .getArray())
                                                   .mapToDouble(Double::doubleValue)
                                                   .toArray()));
            }
        }
        finally
        {
            if (resultingPairs != null)
            {
                resultingPairs.close();
            }

            if (connection != null)
            {
                connection.close();
            }
        }

        this.executeOnComplete();
        return pairs;
    }

    private final int progress;
    private final ProjectConfig projectConfig;

    @Override
    protected String getTaskName () {
        return "PairRetriever: Step " + String.valueOf(this.progress) + " for " + this.projectConfig.getLabel();
    }
}
