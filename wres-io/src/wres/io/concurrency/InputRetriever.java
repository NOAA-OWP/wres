package wres.io.concurrency;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
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

/**
 * Created by ctubbs on 7/17/17.
 */
@Internal(exclusivePackage = "wres.io")
public final class InputRetriever extends WRESCallable<MetricInput<?>>
{

    /**
     * Used to save off sets of metric input data without storing them within
     * a MetricInput object (where we don't have a clean way of accessing the
     * data within
     */
    private static class InputPair
    {
        public InputPair(Integer window, double left, Double[] right)
        {
            this.window = window;
            this.right = right;
            this.left = left;
        }

        public double getLeft()
        {
            return this.left;
        }

        public Double[] getRight()
        {
            return this.right;
        }

        public Integer getWindow()
        {
            return this.window;
        }

        private final Integer window;
        private final double left;
        private final Double[] right;
    }


    private static final ConcurrentHashMap<Conditions.Feature, Map<String, List<InputPair>>>
            savedPairing = new ConcurrentHashMap<>(  );

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

    /**
     * Writes any and all saved pair data to the specified location as CSV data
     * @param destination The place to save the data to. The CSV extension
     *                    will be added if it isn't present.
     */
    public static void outputSavedPairs(String destination)
    {
        if (!destination.endsWith( ".csv" ))
        {
            destination += ".csv";
        }

        try ( BufferedWriter writer = new BufferedWriter( new FileWriter( destination, false ) ))
        {
            writer.write( "Feature,Date,Window,Left,Right" );
            writer.newLine();
            writer.flush();

            // Create a line in the CSV for every set of primary pairs
            // pertaining to every window for every location
            for ( Map.Entry<Conditions.Feature, Map<String, List<InputPair>>> featureMapEntry : savedPairing.entrySet())
            {
                for (Map.Entry<String, List<InputPair>> pairing : featureMapEntry.getValue().entrySet())
                {

                    for(InputPair inputPair : pairing.getValue())
                    {
                        StringJoiner line = new StringJoiner( "," );
                        StringJoiner arrayJoiner =
                                new StringJoiner( ",", "\"[", "]\"" );

                        // Add the location information
                        line.add( ConfigHelper.getFeatureDescription(
                                featureMapEntry.getKey() ) );

                        // Add the date
                        line.add( pairing.getKey().toString() );

                        // Add the window number
                        line.add(String.valueOf( inputPair.getWindow() ));

                        // Add the left hand value
                        line.add(String.valueOf(inputPair.getLeft()));

                        for (int rightIndex = 0; rightIndex < inputPair.getRight().length; ++rightIndex)
                        {
                            arrayJoiner.add( String.valueOf( inputPair.getRight()[rightIndex] ));
                        }

                        // Add the array representation
                        line.add(arrayJoiner.toString());

                        // Append the new line to the file
                        writer.write( line.toString() );
                        writer.newLine();
                        writer.flush();
                    }
                }
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    /**
     * Save data into a persistent collection of data about what was paired
     * @param feature
     * @param date
     * @param window
     * @param left
     * @param right
     */
    private static void savePairInfo( Conditions.Feature feature,
                                     String date,
                                     Integer window,
                                     double left,
                                     Double[] right)
    {
        synchronized (savedPairing)
        {
            savedPairing.putIfAbsent( feature, new TreeMap<>() );
            savedPairing.get( feature ).putIfAbsent( date, new ArrayList<>() );
            savedPairing.get( feature ).get( date ).add( new InputPair( window, left, right ) );
        }
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

    public void setSavePairData(boolean savePairData)
    {
        this.savePairData = savePairData;
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
                                                               .getValue() );
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

                if (this.savePairData && ConfigHelper.isRight( dataSourceConfig, this.projectConfig ))
                {
                    InputRetriever.savePairInfo( this.feature, date, this.progress, leftValue, measurements );
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
    private String zeroDate;
    private boolean savePairData;

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
