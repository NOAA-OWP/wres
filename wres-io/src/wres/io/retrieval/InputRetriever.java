package wres.io.retrieval;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
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
import wres.datamodel.DataFactory;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.Dimension;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.io.concurrency.Executor;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.UnitConversions;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.io.utilities.ScriptGenerator;
import wres.io.writing.PairWriter;
import wres.util.Internal;
import wres.util.Strings;
import wres.util.Time;

/**
 * Created by ctubbs on 7/17/17.
 */
@Internal(exclusivePackage = "wres.io")
class InputRetriever extends WRESCallable<MetricInput<?>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InputRetriever.class);

    @Internal(exclusivePackage = "wres.io")
    public InputRetriever (ProjectDetails projectDetails,
                           BiFunction<String, String, List<Double>> getLeftValues)
    {
        this.projectDetails = projectDetails;
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

    private UnitConversions.Conversion getConversion(int measurementUnitID)
    {
        if (this.conversionMap == null)
        {
            this.conversionMap = new TreeMap<>(  );
        }

        if (!this.conversionMap.containsKey( measurementUnitID ))
        {
            this.conversionMap.put(measurementUnitID,
                                   UnitConversions.getConversion( measurementUnitID,
                                                                  this.projectDetails.getDesiredMeasurementUnit() ));
        }

        return this.conversionMap.get( measurementUnitID );
    }

    private Double convertMeasurement(Double value, int measurementUnitID)
    {
        Double convertedMeasurement = null;
        UnitConversions.Conversion conversion = this.getConversion( measurementUnitID );

        if (value != null && !value.isNaN() && conversion != null)
        {
            convertedMeasurement = conversion.convert( value );
        }
        else
        {
            convertedMeasurement = Double.NaN;
        }

        if (convertedMeasurement < this.projectDetails.getMinimumValue() ||
                convertedMeasurement > this.projectDetails.getMaximumValue())
        {
            convertedMeasurement = Double.NaN;
        }

        return convertedMeasurement;
    }

    @Override
    public MetricInput<?> execute() throws Exception
    {
        this.primaryPairs = this.createPairs(this.projectDetails.getRight());

        if (this.projectDetails.hasBaseline())
        {
            this.baselinePairs = this.createPairs(this.projectDetails.getBaseline());
        }

        MetricInput<?> input;

        try
        {
            input = createInput();
        }
        catch ( Exception error )
        {
            LOGGER.debug( Strings.getStackTrace( error ) );
            throw error;
        }
        return input;
    }

    private MetricInput<?> createInput() throws NoDataException
    {
        MetricInput<?> input = null;

        DatasourceType dataType = this.projectDetails.getRight().getType();

        DataFactory factory = DefaultDataFactory.getInstance();

        Metadata metadata = this.buildMetadata(factory, this.projectDetails.getRight());
        Metadata baselineMetadata = null;

        if (this.primaryPairs.size() == 0)
        {
            throw new NoDataException( "No data could be retrieved for Metric calculation for window " +
                                       this.progress +
                                       " for " +
                                       this.projectDetails.getRightVariableName() +
                                       " at " +
                                       ConfigHelper.getFeatureDescription( this.feature ) );
        }

        if (this.projectDetails.hasBaseline())
        {
            baselineMetadata = this.buildMetadata(factory, this.projectDetails.getBaseline());
        }

        if (dataType == DatasourceType.ENSEMBLE_FORECASTS)
        {
            input = factory.ofEnsemblePairs(this.primaryPairs, this.baselinePairs, metadata, baselineMetadata, this.climatology);
        }
        else
        {
            List<PairOfDoubles> primary = convertToPairOfDoubles( this.primaryPairs );
            List<PairOfDoubles> baseline = null;

            if (this.baselinePairs != null && this.baselinePairs.size() > 0)
            {
                baseline = convertToPairOfDoubles( this.baselinePairs );
            }

            try
            {

                input = factory.ofSingleValuedPairs( primary,
                                                     baseline,
                                                     metadata,
                                                     baselineMetadata,
                                                     this.climatology );
            }
            catch (MetricInputException mie)
            {
                LOGGER.error("A collection of pairs could not be created at " +
                             "window {} for '{}'",
                             this.progress + 1,
                             ConfigHelper.getFeatureDescription( this.feature ));
                LOGGER.debug(Strings.getStackTrace( mie ));
            }
        }

        return input;
    }

    private List<PairOfDoubles> convertToPairOfDoubles(List<PairOfDoubleAndVectorOfDoubles> multiValuedPairs)
    {
        List<PairOfDoubles> pairs = new ArrayList<>(  );

        DataFactory factory = DefaultDataFactory.getInstance();

        for (PairOfDoubleAndVectorOfDoubles pair : multiValuedPairs)
        {
            for (double pairedValue : pair.getItemTwo())
            {
                pairs.add(factory.pairOf( pair.getItemOne(), pairedValue ));
            }
        }

        return pairs;
    }

    private String getLoadScript(DataSourceConfig dataSourceConfig)
            throws SQLException, InvalidPropertiesFormatException,
            NoDataException
    {
        String loadScript;

        if ( this.projectDetails.getRight().equals(dataSourceConfig))
        {
            if (this.rightLoadScript == null)
            {
                this.rightLoadScript = ScriptGenerator.generateLoadDatasourceScript( this.projectDetails,
                                                                                     dataSourceConfig,
                                                                                     this.feature,
                                                                                     this.progress);
            }
            loadScript = this.rightLoadScript;
        }
        else
        {
            if (this.baselineLoadScript == null)
            {
                this.baselineLoadScript = ScriptGenerator.generateLoadDatasourceScript( this.projectDetails,
                                                                                        dataSourceConfig,
                                                                                        this.feature,
                                                                                        this.progress);
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
                if (aggHour != null && resultSet.getInt( "agg_hour" ) <= aggHour)
                {
                    pairs = this.addPair( pairs, date, rightValues, dataSourceConfig );

                    rightValues = new TreeMap<>(  );
                }

                aggHour = resultSet.getInt( "agg_hour" );

                date = resultSet.getString("value_date");

                Double[] measurements = (Double[])resultSet.getArray("measurements").getArray();

                for (int measurementIndex = 0; measurementIndex < measurements.length; ++measurementIndex)
                {
                    Integer measurementUnitID = resultSet.getInt( "measurementunit_id" );
                    rightValues.putIfAbsent( measurementIndex, new ArrayList<>() );
                    rightValues.get(measurementIndex)
                               .add( this.convertMeasurement( measurements[measurementIndex],
                                                              measurementUnitID ) );
                }
            }

            pairs = this.addPair( pairs, date, rightValues, dataSourceConfig );
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

        return Collections.unmodifiableList( pairs );
    }

    private List<PairOfDoubleAndVectorOfDoubles> addPair( List<PairOfDoubleAndVectorOfDoubles> pairs,
                                                          String date,
                                                          Map<Integer, List<Double>> rightValues,
                                                          DataSourceConfig dataSourceConfig)
            throws ProjectConfigException, NoDataException,
            InvalidPropertiesFormatException
    {
        if (rightValues.size() > 0)
        {
            PairOfDoubleAndVectorOfDoubles pair = this.getPair( date, rightValues );
            if (pair != null)
            {
                writePair( date, pair, dataSourceConfig );
                pairs.add( pair );
            }
        }
        return pairs;
    }

    private Metadata buildMetadata (DataFactory dataFactory, DataSourceConfig sourceConfig)
    {
        MetadataFactory metadataFactory = dataFactory.getMetadataFactory();
        Dimension dim = metadataFactory.getDimension( this.projectDetails.getDesiredMeasurementUnit());

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
                windowWidth = this.projectDetails.getWindowWidth() * 1.0;
            }
            catch ( InvalidPropertiesFormatException e )
            {
                LOGGER.error( Strings.getStackTrace(e) );
                LOGGER.error("The width of the standard window for this project could not be determined.");
            }
        }

        Double lastLead = ( windowNumber * windowWidth ) + leadOffset;
        return metadataFactory.getMetadata( dim,
                                            datasetIdentifier,
                                            ConfigHelper.getTimeWindow( this.projectDetails,
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

        String firstDate = Time.minus( date,
                                       this.projectDetails.getAggregationUnit(),
                                       this.projectDetails.getAggregationPeriod());

        List<Double> leftValues = this.getLeftValues.apply( firstDate, date );

        if (leftValues == null || leftValues.size() == 0)
        {
            LOGGER.trace( "No values from the left could be retrieved to pair with the retrieved right values." );
            return null;
        }

        Double leftAggregation =
                wres.util.Collections.aggregate( leftValues,
                                                 this.projectDetails.getAggregationFunction() );

        /*if ( leftAggregation.isNaN())
        {
            LOGGER.debug("The left value aggregated to NaN and could not form a pair from the dates '{}' to '{}'.",
                         firstDate,
                         date);
            return null;
        }*/

        Double[] rightAggregation = new Double[rightValues.size()];

        byte memberIndex = 0;
        for (List<Double> values : rightValues.values())
        {
            rightAggregation[memberIndex] =
                    wres.util.Collections.aggregate( values,
                                                     this.projectDetails.getAggregationFunction() );
            memberIndex++;
        }

        List<Double> validAggregations = new ArrayList<>();

        for (Double value : rightAggregation)
        {
            if (value == null)
            {
                value = Double.NaN;
            }

            validAggregations.add( value );
        }

        return DefaultDataFactory.getInstance().pairOf( leftAggregation,
                                                        validAggregations.toArray( new Double[validAggregations.size()] ) );
    }

    private void writePair( String date,
                            PairOfDoubleAndVectorOfDoubles pair,
                            DataSourceConfig dataSourceConfig )
            throws ProjectConfigException
    {
        boolean isBaseline = dataSourceConfig.equals( this.projectDetails.getBaseline() );
        List<DestinationConfig> destinationConfigs = this.projectDetails.getPairDestinations();

        for ( DestinationConfig dest : destinationConfigs )
        {
            PairWriter saver = new PairWriter( dest,
                                               date,
                                               this.feature,
                                               this.progress,
                                               pair,
                                               isBaseline );
            Executor.submitHighPriorityTask( saver);
        }
    }

    private String baselineLoadScript;
    private String rightLoadScript;
    private int leadOffset;
    private int progress;
    private final ProjectDetails projectDetails;
    private Feature feature;
    private final BiFunction<String, String, List<Double>> getLeftValues;
    private VectorOfDoubles climatology;
    private List<PairOfDoubleAndVectorOfDoubles> primaryPairs;
    private List<PairOfDoubleAndVectorOfDoubles> baselinePairs;
    private Map<Integer, UnitConversions.Conversion> conversionMap;

    @Override
    protected Logger getLogger()
    {
        return InputRetriever.LOGGER;
    }
}
