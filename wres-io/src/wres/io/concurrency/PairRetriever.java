package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.config.generated.Conditions;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metric.*;
import wres.io.data.caching.UnitConversions;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptGenerator;
import wres.util.Internal;
import wres.util.NotImplementedException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Created by ctubbs on 7/17/17.
 */
@Internal(exclusivePackage = "wres.io")
public final class PairRetriever extends WRESCallable<MetricInput<?>>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(PairRetriever.class);

    @Internal(exclusivePackage = "wres.io")
    public PairRetriever(ProjectConfig projectConfig, Conditions.Feature feature, int progress)
    {
        this.projectConfig = projectConfig;
        this.feature = feature;
        this.progress = progress;
    }

    @Override
    public MetricInput<?> execute() throws Exception
    {
        List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();

        Connection connection = null;
        ResultSet resultingPairs = null;
        final String script = ScriptGenerator.generateGetPairData(this.projectConfig, this.feature, this.progress);
        try
        {
            connection = Database.getConnection();
            resultingPairs = Database.getResults(connection, script);

            while(resultingPairs.next())
            {
                pairs.add(createPair(resultingPairs));
            }
        }
        finally
        {
            if(resultingPairs != null)
            {
                resultingPairs.close();
            }

            if(connection != null)
            {
                Database.returnConnection(connection);
            }
        }

        return createInput(pairs);
    }

    private MetricInput<?> createInput(List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        MetricInput<?> input = null;

        DatasourceType dataType = this.projectConfig.getInputs().getRight().getType();

        DataFactory factory = DefaultDataFactory.getInstance();

        Metadata metadata = this.buildMetadata(factory);

        // TODO: Handle baseline pairs
        // TODO: Handle addition of climatology for probability thresholds on pair construction
        if (dataType == DatasourceType.ENSEMBLE_FORECASTS)
        {
            input = factory.ofEnsemblePairs(pairs, metadata);
        }
        else
        {
            input = factory.ofSingleValuedPairs(factory.getSlicer().transformPairs(pairs,
                                                                                   factory.getSlicer()::transformPair),
                                                metadata);
        }

        return input;
    }

    private Metadata buildMetadata(DataFactory dataFactory)
    {
        MetadataFactory metadataFactory = dataFactory.getMetadataFactory();
        Dimension dim = metadataFactory.getDimension(String.valueOf(this.projectConfig.getPair().getUnit()));

        // TODO: Build ConfigHelper method to get the identifier, but doesn't rely on having a location
        String geospatialIdentifier = this.feature.getLocation().getLid();
        String variableIdentifier = this.projectConfig.getInputs().getRight().getVariable().getValue();

        // TODO: need to add scenario IDs for the main data and any baseline. Also need to use a general identifier for
        // the left/right variable rather than projectConfig.getInputs().getRight().getVariable().getValue()
        // To be replaced by non-generic ID from the Project Configuration
        String scenarioIdentifier = "model"; //this.projectConfig.getInputs().getRight().getLabel();

        // TO be replaced by non-generic ID from the Project Configuration
        String baselineScenarioID = null;

        DataSourceConfig baseline = projectConfig.getInputs().getBaseline();

        if (Objects.nonNull(baseline) && !baseline.getSource().isEmpty())
        {
            baselineScenarioID = "baseline model";
        }

        DatasetIdentifier datasetIdentifier = metadataFactory.getDatasetIdentifier(geospatialIdentifier,
                                                                                   variableIdentifier,
                                                                                   scenarioIdentifier,
                                                                                   baselineScenarioID);

        return metadataFactory.getMetadata(dim, datasetIdentifier, this.progress);
    }

    private PairOfDoubleAndVectorOfDoubles createPair(ResultSet row) throws SQLException, NotImplementedException
    {
        final DataFactory dataFactory = DefaultDataFactory.getInstance();
        Double[] measurements = (Double[])row.getArray("measurements").getArray();
        double[] convertedMeasurements = new double[measurements.length];

        int sourceOneUnitID = row.getInt("sourceOneMeasurementUnitID");
        int sourceTwoUnitID = row.getInt("sourceTwoMeasurementUnitID");
        String desiredMeasurementUnit = this.projectConfig.getPair().getUnit();


        for(int measurementIndex = 0; measurementIndex < measurements.length; ++measurementIndex)
        {
            convertedMeasurements[measurementIndex] = UnitConversions.convert(measurements[measurementIndex],
                                                                              sourceTwoUnitID,
                                                                              desiredMeasurementUnit);
        }

        double convertedSourceOne = UnitConversions.convert(row.getDouble("sourceOneValue"),
                                                            sourceOneUnitID,
                                                            desiredMeasurementUnit);

        return dataFactory.pairOf(convertedSourceOne, convertedMeasurements);
    }

    private final int progress;
    private final ProjectConfig projectConfig;
    private final Conditions.Feature feature;

    @Override
    protected String getTaskName()
    {
        return "PairRetriever: Step " + String.valueOf(this.progress);
    }

    @Override
    protected Logger getLogger()
    {
        return PairRetriever.LOGGER;
    }
}
