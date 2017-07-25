package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.config.generated.ProjectConfig;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DefaultDataFactory;
import wres.io.utilities.ScriptGenerator;
import wres.io.utilities.Database;
import wres.util.Internal;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by ctubbs on 7/17/17.
 */
@Internal(exclusivePackage = "wres.io")
public final class PairRetriever extends WRESCallable<List<PairOfDoubleAndVectorOfDoubles>>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(PairRetriever.class);

    @Internal(exclusivePackage = "wres.io")
    public PairRetriever(ProjectConfig projectConfig, int progress)
    {
        this.projectConfig = projectConfig;
        this.progress = progress;
    }

    @Override
    public List<PairOfDoubleAndVectorOfDoubles> execute () throws Exception {
        List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();

        Connection connection = null;
        ResultSet resultingPairs = null;

        final DataFactory dataFactory = DefaultDataFactory.getInstance();

        try
        {
            final String script = ScriptGenerator.generateGetPairData(this.projectConfig, this.progress);
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

        return pairs;
    }

    private final int progress;
    private final ProjectConfig projectConfig;

    @Override
    protected String getTaskName () {
        return "PairRetriever: Step " + String.valueOf(this.progress) + " for " + this.projectConfig.getLabel();
    }

    @Override
    protected Logger getLogger () {
        return PairRetriever.LOGGER;
    }
}
