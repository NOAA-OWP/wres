package wres.io.concurrency;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DestinationConfig;
import wres.config.generated.Feature;
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
import wres.util.Strings;

/**
 * Created by ctubbs on 7/17/17.
 */
@Internal(exclusivePackage = "wres.io")
public final class InputRetriever extends WRESCallable<MetricInput<?>>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(InputRetriever.class);

    // TODO: Gross! Cut down to an init function. >3 parameters = no bueno.
    @Internal(exclusivePackage = "wres.io")
    public InputRetriever (ProjectConfig projectConfig,
                           Feature rightFeature,
                           Feature baselineFeature,
                           int progress,
                           Function<String, Double> getLeftValue,
                           VectorOfDoubles climatology)
    {
        this.projectConfig = projectConfig;
        this.rightFeature = rightFeature;
        this.baselineFeature = baselineFeature;
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
                                             ConfigHelper.getFeatureDescription( this.rightFeature ) );
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
                                                             this.getFeature( dataSourceConfig ),
                                                             this.progress,
                                                             this.zeroDate);
    }

    private List<PairOfDoubleAndVectorOfDoubles> createPairs(DataSourceConfig dataSourceConfig)
            throws InvalidPropertiesFormatException, SQLException,
            ProjectConfigException
    {
        List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        String loadScript = getLoadScript( dataSourceConfig );

        Connection connection = null;
        ResultSet resultSet = null;

        Feature feature = this.getFeature( dataSourceConfig );

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

                List<DestinationConfig> destinationConfigs =
                        ConfigHelper.getPairDestinations( this.projectConfig );

                for ( DestinationConfig dest : destinationConfigs )
                {

                    PairWriter saver = new PairWriter();
                    File directoryLocation =
                            ConfigHelper.getDirectoryFromDestinationConfig( dest );
                    saver.setFileDestination( directoryLocation.toString()
                                              + "/pairs.csv" );
                    saver.setFeatureDescription( ConfigHelper.getFeatureDescription( feature ) );
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
        Feature feature = this.getFeature( sourceConfig );

        MetadataFactory metadataFactory = dataFactory.getMetadataFactory();
        Dimension dim = metadataFactory.getDimension(String.valueOf(this.projectConfig.getPair().getUnit()));

        String geospatialIdentifier = ConfigHelper.getFeatureDescription(feature);
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

        if (sourceConfig.getTimeAggregationDescription() == null)
        {
            sourceConfig = this.projectConfig.getInputs().getRight();
        }

        Double windowWidth = 1.0;

        try
        {
            windowWidth = ConfigHelper.getWindowWidth( ConfigHelper.getTimeAggregation( sourceConfig ) );
        }
        catch ( InvalidPropertiesFormatException e )
        {
            LOGGER.error( Strings.getStackTrace(e) );
            LOGGER.error("The width of the standard window for this project could not be determined.");
        }

        Double lastLead = windowNumber  * windowWidth;
        return metadataFactory.getMetadata(dim,
                                           datasetIdentifier,
                                           lastLead.intValue());
    }

    private Feature getFeature(DataSourceConfig dataSourceConfig)
    {
        Feature feature;

        if ( ConfigHelper.isRight( dataSourceConfig, this.projectConfig ))
        {
            feature = this.rightFeature;
        }
        else
        {
            feature = this.baselineFeature;
        }

        return feature;
    }

    private final int progress;
    private final ProjectConfig projectConfig;
    private final Feature rightFeature;
    private final Feature baselineFeature;
    private final Function<String, Double> getLeftValue;
    private final VectorOfDoubles climatology;
    private List<PairOfDoubleAndVectorOfDoubles> primaryPairs;
    private List<PairOfDoubleAndVectorOfDoubles> baselinePairs;
    private String zeroDate;

    private Boolean baselineExists()
    {
        return this.baselineFeature != null;
    }

    @Override
    protected Logger getLogger()
    {
        return InputRetriever.LOGGER;
    }
}
