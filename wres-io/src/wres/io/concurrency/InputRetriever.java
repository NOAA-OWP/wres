package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.config.generated.Conditions;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.*;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.UnitConversions;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptGenerator;
import wres.util.Internal;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.function.Function;

/**
 * Created by ctubbs on 7/17/17.
 */
@Internal(exclusivePackage = "wres.io")
public final class InputRetriever extends WRESCallable<MetricInput<?>>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(InputRetriever.class);

    @Internal(exclusivePackage = "wres.io")
    public InputRetriever (ProjectConfig projectConfig, Conditions.Feature feature, int progress, Function<String, Double> getLeftValue, VectorOfDoubles climatology)
    {
        this.projectConfig = projectConfig;
        this.feature = feature;
        this.progress = progress;
        this.getLeftValue = getLeftValue;
        this.climatology = climatology;
    }

    @Override
    public MetricInput<?> execute() throws Exception
    {
        this.primaryPairs = this.createPairs(this.projectConfig.getInputs().getRight());

        if (ConfigHelper.hasBaseline(this.projectConfig)) {
            this.baselinePairs = this.createPairs(this.projectConfig.getInputs().getBaseline());
        }

        return createInput();
    }

    private MetricInput<?> createInput()
    {
        MetricInput<?> input;

        DatasourceType dataType = this.projectConfig.getInputs().getRight().getType();

        DataFactory factory = DefaultDataFactory.getInstance();

        Metadata metadata = this.buildMetadata(factory, this.projectConfig.getInputs().getRight());
        Metadata baselineMetadata = null;

        if (this.baselineExists())
        {
            baselineMetadata = this.buildMetadata(factory, this.projectConfig.getInputs().getBaseline());
        }

        if (dataType == DatasourceType.ENSEMBLE_FORECASTS)
        {
            input = factory.ofEnsemblePairs(this.primaryPairs, this.baselinePairs, metadata, baselineMetadata, this.climatology);
        }
        else
        {
            List<PairOfDoubles> primary = factory.getSlicer().transformPairs(this.primaryPairs, factory.getSlicer()::transformPair);
            List<PairOfDoubles> baseline = null;

            if (this.baselinePairs != null && this.baselinePairs.size() > 0)
            {
                baseline = factory.getSlicer().transformPairs(this.baselinePairs, factory.getSlicer()::transformPair);
            }

            input = factory.ofSingleValuedPairs(primary,
                                                baseline,
                                                metadata,
                                                baselineMetadata,
                                                this.climatology);
        }

        if (this.primaryPairs.size() == 0)
        {
            LOGGER.error("");
            LOGGER.error("THERE IS NO DATA FOR THE INPUT RETRIEVER FOR WINDOW {}", this.progress);
            LOGGER.error("");
        }

        return input;
    }

    private List<PairOfDoubleAndVectorOfDoubles> createPairs(DataSourceConfig dataSourceConfig)
            throws InvalidPropertiesFormatException, SQLException
    {
        List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        String loadScript = ScriptGenerator.generateLoadDatasourceScript(this.projectConfig,
                                                                         dataSourceConfig,
                                                                         this.feature,
                                                                         this.progress);

        Connection connection = null;
        ResultSet resultSet = null;

        DataFactory factory = DefaultDataFactory.getInstance();

        try
        {
            connection = Database.getConnection();
            resultSet = Database.getResults(connection, loadScript);

            while(resultSet.next())
            {
                String date = resultSet.getString("value_date");
                Double leftValue = this.getLeftValue.apply(date);

                // TODO: This is where we'd handle missing values; for now, we're skipping it
                if (leftValue == null)
                {
                    continue;
                }

                Double[] measurements = (Double[])resultSet.getArray("measurements").getArray();

                for (int measurementIndex = 0; measurementIndex < measurements.length; ++measurementIndex)
                {
                    measurements[measurementIndex] = UnitConversions.convert(measurements[measurementIndex],
                                                                             resultSet.getInt("measurementunit_id"),
                                                                             this.projectConfig.getPair().getUnit());
                }

                pairs.add(factory.pairOf(leftValue, measurements));
            }
        }
        finally
        {
            if (resultSet != null)
            {
                resultSet.close();
            }

            if (connection != null)
            {
                Database.returnConnection(connection);
            }
        }

        return pairs;
    }

    private Metadata buildMetadata (DataFactory dataFactory, DataSourceConfig sourceConfig)
    {
        MetadataFactory metadataFactory = dataFactory.getMetadataFactory();
        Dimension dim = metadataFactory.getDimension(String.valueOf(this.projectConfig.getPair().getUnit()));

        String geospatialIdentifier = ConfigHelper.getFeatureDescription(this.feature);
        String variableIdentifier = sourceConfig.getVariable().getValue();

        // TODO: need to add scenario IDs for the main data and any baseline. Also need to use a general identifier for
        // the left/right variable rather than projectConfig.getInputs().getRight().getVariable().getValue()
        // To be replaced by non-generic ID from the Project Configuration
        DatasetIdentifier datasetIdentifier = metadataFactory.getDatasetIdentifier(geospatialIdentifier,
                                                                                   variableIdentifier,
                                                                                   sourceConfig.getLabel());

        return metadataFactory.getMetadata(dim, datasetIdentifier, this.progress);
    }

    private final int progress;
    private final ProjectConfig projectConfig;
    private final Conditions.Feature feature;
    private final Function<String, Double> getLeftValue;
    private final VectorOfDoubles climatology;
    private List<PairOfDoubleAndVectorOfDoubles> primaryPairs;
    private List<PairOfDoubleAndVectorOfDoubles> baselinePairs;
    private Boolean hasBaseline;

    private Boolean baselineExists()
    {
        if (this.hasBaseline == null)
        {
            this.hasBaseline = ConfigHelper.hasBaseline(projectConfig);
        }

        return this.hasBaseline;
    }

    @Override
    protected Logger getLogger()
    {
        return InputRetriever.LOGGER;
    }
}
