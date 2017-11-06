package wres.io.concurrency;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
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
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
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

    public void setFeature(Feature feature)
    {
        this.feature = feature;
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
        String loadScript;

        if (ConfigHelper.isRight( dataSourceConfig, this.projectConfig ))
        {
            if (this.rightLoadScript == null)
            {
                this.rightLoadScript = ScriptGenerator.generateLoadDatasourceScript( this.projectConfig,
                                                                                     dataSourceConfig,
                                                                                     this.feature,
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
                                                                                        this.feature,
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
            ProjectConfigException, NoDataException
    {
        List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        String loadScript = getLoadScript( dataSourceConfig );

        Connection connection = null;
        ResultSet resultSet = null;

        Integer aggHour = null;
        String date = null;

        Map<Integer, List<Double>> rightValues = new TreeMap<>();
        Map<Integer, UnitConversions.Conversion> conversionMap = new TreeMap<>(  );

        /**
         * Task #39440
         *
         * Alternate Solution (for forecasts):
         * 1) Get ids for all time series
         * 2) Instead of doing the full series of joins then the array
         *    aggregation and sorting, group by time series and return a series
         *    of pairs of (timeseries_id, aggregate for all leads)
         *    To do so, the ranges for all timeseries_ids will need to be
         *    determined and added to the where clause. Your query will look
         *    like:
         *
         *    SELECT FV.timeseries_id, AVG(FV.forecasted_value) AS measurement
         *    FROM wres.ForecastValue FV
         *    WHERE (
         *          (FV.timeseries_id >= 1 AND FV.timeseries_id <= 4000)
         *              OR (FV.timeseries_id >= 9000 AND FV.timeseries_id <= 25000)
         *        )
         *        AND FV.lead >= 24
         *        AND FV.lead <= 48;
         *
         * 3) Perform the unit conversion on the aggregated values
         * 4) Determine if the converted aggregation is a valid value for the pull
         *    (is greater than or equal to min value or less than or equal to
         *        the max value)
         * 5) Group all aggregated values in ensemble order
         *    in collections based on the timeseries initialization date +
         *    the maximum number of lead hours for the window.
         * 6) After finding the matching left hand value for each pair of
         *    (date, [aggregated ensemble/single values...]), convert the
         *    left and pair to PairOfDoubleAndVectorOfDouble
         * 7) Add pair object to pair collection
         *
         * Pros:
         *    -  This should provide a performance improvement on databases
         *       with a large amount of data for many projects
         *    -  For a single retrieval on such a system, the current process
         *       takes ~25 seconds. By using the above query, it took 4.03
         *       seconds. For a single lead, it took 1.359 seconds.
         * Cons:
         *    -  More intermediate steps are required
         *    -  Even more complicated
         *    -  Ensembles and their IDs will need to be associated with their
         *       timeseries_ids
         *    -  A listing of acceptable timeseries_ids will need to be sorted
         *       and added to ranges of (min, max) and added to a collection,
         *       then that collection will need to be used to form the
         *
         *       (FV.timeseries_id >= min AND FV.timeseries_id <= max) || ...
         *
         *       statements. The min/max statements will need to have
         *       branching logic so that it is simply
         *
         *       FV.timeseries_id >= min AND FV.timeseries_id <= max
         *
         *       in cases of a single range.
         *
         *    -  Most likely a larger memory footprint
         *    -  Current logic for simulation data will still need to be intact
         *    -  Refactoring for new logic would require a complete rewrite
         *       and forking of several core functions
         */

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
                if (rightValues.size() > 0 && aggHour != null && resultSet.getInt( "agg_hour" ) <= aggHour)
                {
                    PairOfDoubleAndVectorOfDoubles pair = this.getPair( date, rightValues );
                    if (pair != null)
                    {
                        writePair( date, pair, dataSourceConfig );
                        pairs.add( pair );
                    }

                    rightValues = new TreeMap<>(  );
                }

                aggHour = resultSet.getInt( "agg_hour" );

                date = resultSet.getString("value_date");

                Double[] measurements = (Double[])resultSet.getArray("measurements").getArray();

                double minimum = Double.MAX_VALUE * -1.0;
                double maximum = Double.MAX_VALUE;

                if ( this.projectConfig.getPair()
                                       .getValues() != null )
                {
                    if ( this.projectConfig.getPair()
                                           .getValues()
                                           .getMinimum() != null )
                    {
                        minimum = this.projectConfig.getPair()
                                                    .getValues()
                                                    .getMinimum();
                    }

                    if ( this.projectConfig.getPair()
                                           .getValues()
                                           .getMaximum() != null )
                    {
                        maximum = this.projectConfig.getPair()
                                                    .getValues()
                                                    .getMaximum();
                    }
                }

                for (int measurementIndex = 0; measurementIndex < measurements.length; ++measurementIndex)
                {
                    UnitConversions.Conversion conversion = null;
                    Integer measurementUnitID = resultSet.getInt( "measurementunit_id" );
                    if (!conversionMap.containsKey( measurementUnitID ))
                    {
                        conversion = UnitConversions.getConversion( measurementUnitID, this.projectConfig.getPair().getUnit() );
                        conversionMap.put( measurementUnitID, conversion );
                    }
                    else
                    {
                        conversion = conversionMap.get( measurementUnitID );
                    }

                    Double convertedMeasurement = null;

                    if (measurements[measurementIndex] != null)
                    {
                        convertedMeasurement = conversion.convert( measurements[measurementIndex] );
                    }

                    // The value needs to be added if it was null because it indicates
                    // missing source data which will need to be handled.
                    if (convertedMeasurement == null ||
                        (convertedMeasurement >= minimum && maximum >= convertedMeasurement))
                    {
                        rightValues.putIfAbsent( measurementIndex, new ArrayList<>() );
                        rightValues.get(measurementIndex).add( convertedMeasurement );
                    }
                }
            }

            if (rightValues.size() > 0)
            {
                PairOfDoubleAndVectorOfDoubles pair = this.getPair( date, rightValues );
                if (pair != null)
                {
                    writePair( date, pair, dataSourceConfig );
                    pairs.add( pair );
                }
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

        // This will help us determine when the window will end
        int windowNumber = this.progress + 1;
        Double windowWidth = 1.0;
        // If this is a simulation, there are no windows, so set to 0
        if ( !ConfigHelper.isForecast( sourceConfig ))
        {
            windowNumber = 0;
            windowWidth = 0.0;
        }
        else
        {
            try
            {
                windowWidth = ConfigHelper.getWindowWidth( this.projectConfig );
            }
            catch ( InvalidPropertiesFormatException e )
            {
                LOGGER.error( Strings.getStackTrace(e) );
                LOGGER.error("The width of the standard window for this project could not be determined.");
            }
        }
        //TODO: this class and other classes in IO need to use java.time rather than
        //rolling our own (Jbrown @ 5 Nov 2017)
        Double lastLead = ( windowNumber * windowWidth ) + leadOffset;
        return metadataFactory.getMetadata( dim,
                                            datasetIdentifier,
                                            ConfigHelper.getTimeWindow( this.projectConfig,
                                                                        lastLead.longValue(),
                                                                        ChronoUnit.HOURS ) );
    }

    private PairOfDoubleAndVectorOfDoubles getPair(String date,
                                                   Map<Integer, List<Double>> rightValues)
            throws InvalidPropertiesFormatException, ProjectConfigException,
            NoDataException
    {
        if (rightValues == null || rightValues.size() == 0)
        {
            throw new NoDataException( "No values could be retrieved to pair with with any possible set of left values ." );
        }

        String aggFunction = this.getDesiredAggregation()
                                 .getFunction()
                                 .value();

        String firstDate = Time.minus( date,
                                       this.getDesiredAggregation().getUnit().value(),
                                       this.getDesiredAggregation().getPeriod());

        List<Double> leftValues = this.getLeftValues.apply( firstDate, date );

        if (leftValues == null || leftValues.size() == 0)
        {
            LOGGER.trace( "No values from the left could be retrieved to pair with the retrieved right values." );
            return null;
        }

        double leftAggregation = Collections.aggregate(leftValues,
                                                       aggFunction);

        if (Double.compare( leftAggregation, Double.NaN ) == 0)
        {
            LOGGER.debug("The left value aggregated to NaN and could not form a pair from the dates '{}' to '{}'.",
                         firstDate,
                         date);
            return null;
        }

        Double[] rightAggregation = new Double[rightValues.size()];

        byte memberIndex = 0;
        for (List<Double> values : rightValues.values())
        {
            rightAggregation[memberIndex] = Collections.aggregate( values, aggFunction );
            memberIndex++;
        }

        rightAggregation = Collections.shrink(rightAggregation, Double.NaN);
        return DefaultDataFactory.getInstance().pairOf( leftAggregation, rightAggregation );
    }

    private void writePair(String date, PairOfDoubleAndVectorOfDoubles pair, DataSourceConfig dataSourceConfig)
            throws ProjectConfigException
    {

        List<DestinationConfig> destinationConfigs =
                ConfigHelper.getPairDestinations( this.projectConfig );

        for ( DestinationConfig dest : destinationConfigs )
        {
            PairWriter saver = new PairWriter( dest,
                                               date,
                                               this.feature,
                                               this.progress,
                                               pair );
            Executor.submitHighPriorityTask(saver);
        }
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
    private Feature feature;
    private final BiFunction<String, String, List<Double>> getLeftValues;
    private VectorOfDoubles climatology;
    private List<PairOfDoubleAndVectorOfDoubles> primaryPairs;
    private List<PairOfDoubleAndVectorOfDoubles> baselinePairs;
    private String zeroDate;

    private Boolean baselineExists()
    {
        return this.projectConfig.getInputs().getBaseline() != null;
    }

    @Override
    protected Logger getLogger()
    {
        return InputRetriever.LOGGER;
    }
}
