package wres.io.concurrency;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DestinationConfig;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeAggregationConfig;
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
import wres.io.utilities.NoDataException;
import wres.io.utilities.ScriptGenerator;
import wres.util.Collections;
import wres.util.Internal;
import wres.util.Strings;
import wres.util.Time;

/**
 * Created by ctubbs on 7/17/17.
 */
@Internal(exclusivePackage = "wres.io")
public final class InputRetriever extends WRESCallable<MetricInput<?>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InputRetriever.class);

    @Internal(exclusivePackage = "wres.io")
    public InputRetriever (ProjectConfig projectConfig,
                           BiFunction<String, String, List<Double>> getLeftValues)
    {
        this.projectConfig = projectConfig;
        this.getLeftValues = getLeftValues;
    }

    public void setRightFeature(Feature rightFeature)
    {
        this.rightFeature = rightFeature;
    }

    public void setBaselineFeature(Feature baselineFeature)
    {
        this.baselineFeature = baselineFeature;
    }

    public void setProgress(int progress)
    {
        this.progress = progress;
    }

    public void setClimatology(VectorOfDoubles climatology)
    {
        this.climatology = climatology;
    }

    public void setLeadOffset(int leadOffset)
    {
        this.leadOffset = leadOffset;
    }

    @Override
    public MetricInput<?> execute() throws Exception
    {
        this.primaryPairs = this.createPairs(this.projectConfig.getInputs().getRight());

        if (ConfigHelper.hasBaseline(this.projectConfig)) {
            this.baselinePairs = this.createPairs(this.projectConfig.getInputs().getBaseline());
        }

        MetricInput<?> input;

        try
        {
            input = createInput();
        }
        catch ( Exception error )
        {
            LOGGER.error( Strings.getStackTrace( error ) );
            throw error;
        }
        return input;
    }

    public void setZeroDate(String zeroDate)
    {
        this.zeroDate = zeroDate;
    }

    private MetricInput<?> createInput() throws NoDataException
    {
        MetricInput<?> input;

        DatasourceType dataType = this.projectConfig.getInputs().getRight().getType();

        DataFactory factory = DefaultDataFactory.getInstance();

        Metadata metadata = this.buildMetadata(factory, this.projectConfig.getInputs().getRight());
        Metadata baselineMetadata = null;

        if (this.primaryPairs.size() == 0)
        {
            throw new NoDataException( "No data could be retrieved for Metric calculation for window " +
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
        String loadScript;

        if (ConfigHelper.isRight( dataSourceConfig, this.projectConfig ))
        {
            if (this.rightLoadScript == null)
            {
                this.rightLoadScript = ScriptGenerator.generateLoadDatasourceScript( this.projectConfig,
                                                                                     dataSourceConfig,
                                                                                     this.getFeature( dataSourceConfig ),
                                                                                     this.progress,
                                                                                     this.zeroDate,
                                                                                     this.leadOffset);
            }
            loadScript = this.rightLoadScript;
        }
        else
        {
            if (this.baselineLoadScript == null)
            {
                this.baselineLoadScript = ScriptGenerator.generateLoadDatasourceScript( this.projectConfig,
                                                                                        dataSourceConfig,
                                                                                        this.getFeature( dataSourceConfig ),
                                                                                        this.progress,
                                                                                        this.zeroDate,
                                                                                        this.leadOffset);
            }
            loadScript = this.baselineLoadScript;
        }
        return loadScript;
    }

    // TODO: REFACTOR
    private List<PairOfDoubleAndVectorOfDoubles> createPairs(DataSourceConfig dataSourceConfig)
            throws InvalidPropertiesFormatException, SQLException,
            ProjectConfigException
    {
        List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        String loadScript = getLoadScript( dataSourceConfig );

        boolean isForecast = ConfigHelper.isForecast( dataSourceConfig );

        Connection connection = null;
        ResultSet resultSet = null;

        Integer aggHour = null;
        String startDate = null;
        String date = null;
        String aggFunction = ConfigHelper.getTimeAggregation( this.projectConfig )
                                         .getFunction()
                                         .value();

        List<Double> leftValues;
        Map<Integer, List<Double>> rightValues = new TreeMap<>();

        try
        {
            connection = Database.getConnection();
            resultSet = Database.getResults(connection, loadScript);

            while(resultSet.next())
            {
                    /**
                     * aggHour: The hour into the aggregation
                     * With the grouped aggregation, you might have several
                     * blocks, each with values an hour in, or two hours in,
                     * or three, etc. We don't want to mix those while
                     * aggregating though; we still want to aggregate these
                     * blocks, but we want to keep each chunk separate.
                     *
                     * I tried returning the time of the start of the block,
                     * but the made calculations take ~3.5 minutes for six
                     * months of data, but switching to the agg_hour set up
                     * reduced that to 1.25 minutes.  Due to that speed up,
                     * we are using the agg_hour method instead of the more
                     * straight forward process.
                     */
                    if (aggHour != null && resultSet.getInt( "agg_hour" ) <= aggHour)
                    {
                        PairOfDoubleAndVectorOfDoubles pair = this.getPair( date, rightValues );
                        writePair( date, pair, dataSourceConfig );
                        pairs.add(pair);

                        rightValues = new TreeMap<>(  );
                    }

                    aggHour = resultSet.getInt( "agg_hour" );

                date = resultSet.getString("value_date");

                Double[] measurements = (Double[])resultSet.getArray("measurements").getArray();

                double minimum = Double.MAX_VALUE * -1.0;
                double maximum = Double.MAX_VALUE;

                if (this.projectConfig.getConditions().getValues() != null)
                {
                    if (this.projectConfig.getConditions().getValues().getMinimum() != null)
                    {
                        minimum = this.projectConfig.getConditions().getValues().getMinimum();
                    }

                    if (this.projectConfig.getConditions().getValues().getMaximum() != null)
                    {
                        maximum = this.projectConfig.getConditions().getValues().getMaximum();
                    }
                }

                for (int measurementIndex = 0; measurementIndex < measurements.length; ++measurementIndex)
                {
                    double convertedMeasurement = UnitConversions.convert(measurements[measurementIndex],
                                                                             resultSet.getInt("measurementunit_id"),
                                                                             this.projectConfig.getPair().getUnit());

                    if (convertedMeasurement >= minimum && maximum >= convertedMeasurement)
                    {
                        rightValues.putIfAbsent( measurementIndex, new ArrayList<>() );
                        rightValues.get(measurementIndex).add( convertedMeasurement );
                    }
                }
            }

            if (rightValues.size() > 0)
            {
                PairOfDoubleAndVectorOfDoubles pair = this.getPair( date, rightValues );
                this.writePair( date, pair, dataSourceConfig );
                pairs.add(pair);
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

        Double windowWidth = 1.0;

        try
        {
            windowWidth = ConfigHelper.getWindowWidth( this.projectConfig );
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

    private PairOfDoubleAndVectorOfDoubles getPair(String date,
                                                   Map<Integer, List<Double>> rightValues)
            throws InvalidPropertiesFormatException, ProjectConfigException
    {
        String aggFunction = this.getDesiredAggregation()
                                 .getFunction()
                                 .value();

        String firstDate = Time.minus( date,
                                       this.getDesiredAggregation().getUnit().value(),
                                       this.getDesiredAggregation().getPeriod());

        List<Double> leftValues = this.getLeftValues.apply( firstDate, date );
        double leftAggregation = Collections.aggregate(leftValues,
                                                       aggFunction);
        Double[] rightAggregation = new Double[rightValues.size()];


        for (int memberIndex = 0; memberIndex < rightValues.size(); ++memberIndex)
        {
            rightAggregation[memberIndex] = Collections.aggregate( rightValues.get( memberIndex ), aggFunction );
        }

        return DefaultDataFactory.getInstance().pairOf( leftAggregation, rightAggregation );
    }

    private void writePair(String date, PairOfDoubleAndVectorOfDoubles pair, DataSourceConfig dataSourceConfig)
            throws ProjectConfigException
    {

        List<DestinationConfig> destinationConfigs =
                ConfigHelper.getPairDestinations( this.projectConfig );

        for ( DestinationConfig dest : destinationConfigs )
        {

            PairWriter saver = new PairWriter();
            File directoryLocation =
                    ConfigHelper.getDirectoryFromDestinationConfig( dest );
            saver.setFileDestination( directoryLocation.toString()
                                      + "/pairs.csv" );
            saver.setFeatureDescription( ConfigHelper.getFeatureDescription( this.getFeature( dataSourceConfig ) ) );
            saver.setDate( date );
            saver.setWindowNum( this.progress );
            saver.setLeft( pair.getItemOne() );
            saver.setRight( pair.getItemTwo() );

            Executor.submitHighPriorityTask(saver);
        }
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

    private TimeAggregationConfig getDesiredAggregation()
    {
        return this.projectConfig.getPair().getDesiredTimeAggregation();
    }

    private String baselineLoadScript;
    private String rightLoadScript;
    private int leadOffset;
    private int progress;
    private final ProjectConfig projectConfig;
    private Feature rightFeature;
    private Feature baselineFeature;
    private final BiFunction<String, String, List<Double>> getLeftValues;
    private VectorOfDoubles climatology;
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
