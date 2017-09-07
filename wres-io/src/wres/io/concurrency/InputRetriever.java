package wres.io.concurrency;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Conditions;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.Dimension;
import wres.datamodel.Metadata;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricInput;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfDoubles;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.UnitConversions;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptGenerator;
import wres.util.Internal;
import wres.util.ProgressMonitor;

/**
 * Created by ctubbs on 7/17/17.
 */
@Internal(exclusivePackage = "wres.io")
public final class InputRetriever extends WRESCallable<MetricInput<?>>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(InputRetriever.class);

    @Internal(exclusivePackage = "wres.io")
    public InputRetriever (ProjectConfig projectConfig,
                           Conditions.Feature feature,
                           int progress,
                           Function<String, Double> getLeftValue,
                           VectorOfDoubles climatology)
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

    public void setZeroDate(String zeroDate)
    {
        this.zeroDate = zeroDate;
    }

    private MetricInput<?> createInput()
    {
        MetricInput<?> input;

        DatasourceType dataType = this.projectConfig.getInputs().getRight().getType();

        DataFactory factory = DefaultDataFactory.getInstance();

        Metadata metadata = this.buildMetadata(factory, this.projectConfig.getInputs().getRight());
        Metadata baselineMetadata = null;

        if (this.primaryPairs.size() == 0)
        {
            throw new IllegalStateException( "No data could be retrieved for Metric calculation for window " +
                                             this.progress +
                                             " for " +
                                             this.projectConfig.getInputs()
                                                               .getRight()
                                                               .getVariable()
                                                               .getValue() +
                                             " at " +
                                             ConfigHelper.getFeatureDescription( this.feature ) );
        }

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

        return input;
    }

    private String getLoadScript(DataSourceConfig dataSourceConfig)
            throws SQLException, InvalidPropertiesFormatException
    {
        return ScriptGenerator.generateLoadDatasourceScript( this.projectConfig,
                                                             dataSourceConfig,
                                                             this.feature,
                                                             this.progress,
                                                             this.zeroDate);
    }

    private List<PairOfDoubleAndVectorOfDoubles> createPairs(DataSourceConfig dataSourceConfig)
            throws InvalidPropertiesFormatException, SQLException
    {
        List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        String loadScript = getLoadScript( dataSourceConfig );

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

                if ( this.projectConfig.getPair().isSaveToDisk() &&
                     ConfigHelper.isRight( dataSourceConfig, this.projectConfig ))
                {

                    PairWriter saver = new PairWriter();
                    saver.setFileDestination( this.projectConfig.getPair().getSaveLocation() );
                    saver.setFeatureDescription( ConfigHelper.getFeatureDescription( this.feature ) );
                    saver.setDate( date );
                    saver.setWindowNum( this.progress );
                    saver.setLeft( leftValue );
                    saver.setRight( measurements );

                    saver.setOnRun( ProgressMonitor.onThreadStartHandler() );
                    saver.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );

                    Executor.submitHighPriorityTask(saver);
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

        DatasetIdentifier datasetIdentifier = metadataFactory.getDatasetIdentifier(geospatialIdentifier,
                                                                                   variableIdentifier,
                                                                                   sourceConfig.getLabel());

        int windowNumber = this.progress;

        // If this is a simulation, there are no windows, so set to 0
        if ( sourceConfig.getType() == DatasourceType.SIMULATIONS)
        {
            windowNumber = 0;
        }

        return metadataFactory.getMetadata(dim, datasetIdentifier, windowNumber);
    }

    private final int progress;
    private final ProjectConfig projectConfig;
    private final Conditions.Feature feature;
    private final Function<String, Double> getLeftValue;
    private final VectorOfDoubles climatology;
    private List<PairOfDoubleAndVectorOfDoubles> primaryPairs;
    private List<PairOfDoubleAndVectorOfDoubles> baselinePairs;
    private Boolean hasBaseline;
    private String zeroDate;

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
