package wres.io.writing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.Feature;
import wres.config.generated.TimeWindowMode;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;

public class PairWriter extends WRESCallable<Boolean>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( PairWriter.class );
    private static final Object PAIR_OUTPUT_LOCK = new Object();
    private static final String OUTPUT_HEADER = "Feature,Date,Lead,Window,Left,Right";
    private static final String DELIMITER = ",";
    private static final String PAIR_FILENAME = "/pairs.csv";
    private static final String BASELINE_FILENAME = "/baseline_pairs.csv";

    /**
     * Stores a map of open writers so we don't have to constantly reopen the files.
     */
    private static final HashMap<String, BufferedWriter> PATH_NAME_TO_WRITER_MAP = new HashMap<>();

    private static boolean headerHasBeenWritten;
    private static boolean baselineHeaderHasBeenWritten;

    private final DestinationConfig destinationConfig;
    private final Instant date;
    private final Feature feature;
    private final int windowNum;
    private final PairOfDoubleAndVectorOfDoubles pair;
    private final boolean isBaseline;
    private final int poolingStep;
    private final ProjectDetails projectDetails;
    private final int lead;

    private DecimalFormat formatter;


    // TODO: IMPLEMENT BUILDER
    public PairWriter( DestinationConfig destinationConfig,
                       Instant date,
                       Feature feature,
                       int windowNum,
                       PairOfDoubleAndVectorOfDoubles pair,
                       boolean isBaseline,
                       int poolingStep,
                       ProjectDetails projectDetails,
                       int lead)
    {
        this.destinationConfig = destinationConfig;
        this.date = date;
        this.feature = feature;
        this.windowNum = windowNum;
        this.pair = pair;
        this.isBaseline = isBaseline;
        this.poolingStep = poolingStep;
        this.projectDetails = projectDetails;
        this.lead = lead;
    }

    @Override
    protected Boolean execute() throws IOException, ProjectConfigException
    {
        boolean success = false;

        File directoryFromDestinationConfig =
                ConfigHelper.getDirectoryFromDestinationConfig( this.getDestinationConfig() );

        String actualFileDestination = directoryFromDestinationConfig.getCanonicalPath();

        if (this.isBaseline)
        {
            actualFileDestination += BASELINE_FILENAME;
        }
        else
        {
            actualFileDestination += PAIR_FILENAME;
        }

        synchronized ( PAIR_OUTPUT_LOCK )
        {
            if ( (!this.isBaseline && !PairWriter.headerHasBeenWritten) ||
                 (this.isBaseline && !PairWriter.baselineHeaderHasBeenWritten) )
            {
                Files.deleteIfExists( Paths.get( actualFileDestination) );
            }

//            try ( FileWriter fileWriter = new FileWriter( actualFileDestination,
//                                                         true );
//                  BufferedWriter writer = new BufferedWriter( fileWriter ) )
            try
            {
                BufferedWriter writer = obtainWriter( actualFileDestination );
                if ( this.isBaseline && !PairWriter.baselineHeaderHasBeenWritten)
                {
                    writer.write( OUTPUT_HEADER );
                    writer.newLine();

                    PairWriter.baselineHeaderHasBeenWritten = true;
                }
                else if ( !this.isBaseline && !PairWriter.headerHasBeenWritten )
                {
                    writer.write( OUTPUT_HEADER );
                    writer.newLine();

                    PairWriter.headerHasBeenWritten = true;
                }

                StringJoiner line = new StringJoiner( DELIMITER );

                line.add( ConfigHelper.getFeatureDescription( this.getFeature() ) );

                // Avoid changing date format to iso format because benchmarks
                line.add( this.date.toString()
                                   .replace( "T", " " )
                                   .replace( "Z", "" ) );

                // But above could be as simple as this (and be more precise):
                //line.add( this.date.toString() );

                line.add(String.valueOf(this.lead));

                line.add( this.getWindow() );

                line.add(this.getLeftValue());

                line.add(this.getRightValues());

                writer.write( line.toString() );
                writer.newLine();

                success = true;
            }
            catch ( SQLException e )
            {
                LOGGER.error("Pairs could not be written for " +
                             ConfigHelper.getFeatureDescription( this.feature ),
                             e);
            }
        }

        return success;
    }

    @Override
    protected boolean validate()
    {
        if ( this.getDestinationConfig() == null )
        {
            throw new IllegalArgumentException(
                    "The PairWriter does not have a destination to write to." );
        }
        else if ( this.getFeature() == null )
        {
            throw new IllegalArgumentException(
                    "No feature was specified for where pairs belong to." );
        }
        else if ( this.getDate() == null )
        {
            throw new IllegalArgumentException(
                    "No date was specified for when the paired data occurred." );
        }

        try
        {
            ConfigHelper.getDirectoryFromDestinationConfig( this.getDestinationConfig() );
        }
        catch ( ProjectConfigException pce )
        {
            throw new IllegalArgumentException( "The PairWriter needs a valid destination", pce );
        }

        return true;
    }

    @Override
    protected Logger getLogger()
    {
        return LOGGER;
    }

    private DestinationConfig getDestinationConfig()
    {
        return this.destinationConfig;
    }

    private Instant getDate()
    {
        return this.date;
    }

    private Feature getFeature()
    {
        return this.feature;
    }

    private String getWindow() throws SQLException, NoDataException
    {

        int window = this.getWindowNum();

        // If basis time pooling is used, you get intermediary pools. This means
        // that you don't just get entries for window 0, 1, 2, 3, 4, etc, you
        // get window 0 pooling step 1, window 0 pooling step 2, window 1
        // pooling step 1, etc. To find the overall window (i.e. "this is the
        // fifth calculation"), you need to break down the calculation to
        // compensate for the number of intermediate windows
        if ( this.projectDetails.getPoolingMode() == TimeWindowMode.ROLLING )
        {
            window /= this.projectDetails.getLeadFrequency();
            window *= (this.projectDetails.getPoolCount( this.feature ));
            window += this.poolingStep;
        }

        window++;

        return String.valueOf(window);
    }

    private int getWindowNum()
    {
        return this.windowNum;
    }

    private String getLeftValue()
    {

        double leftValue = pair.getItemOne();
        String left;

        if ( Double.compare( leftValue, Double.NaN ) == 0 )
        {
            left = "NaN";
        }
        else if ( this.getFormatter() != null )
        {
            left = this.getFormatter().format( leftValue );
        }
        else
        {
            left = String.valueOf( leftValue ) ;
        }

        return left;
    }

    private String getRightValues()
    {
        double[] rightValues = pair.getItemTwo();
        StringJoiner arrayJoiner = new StringJoiner( DELIMITER );

        Arrays.sort( rightValues );

        for ( Double rightValue : rightValues )
        {
            if ( rightValue.isNaN() )
            {
                arrayJoiner.add( "NaN" );
            }
            else if ( formatter != null )
            {
                arrayJoiner.add( formatter.format( rightValue ) );
            }
            else
            {
                arrayJoiner.add( String.valueOf( rightValue ) );
            }
        }

        return arrayJoiner.toString();
    }

    private DecimalFormat getFormatter()
    {
        if (this.formatter == null)
        {
            String configuredFormat = this.getDestinationConfig().getDecimalFormat();

            if ( configuredFormat != null && !configuredFormat.isEmpty() )
            {
                this.formatter = new DecimalFormat();
                this.formatter.applyPattern( configuredFormat );
            }
        }

        return this.formatter;
    }

    /**
     * @param absPathName
     * @return Either an already open writer or creates a new one.
     * @throws IOException
     */
    private static BufferedWriter obtainWriter(String absPathName) throws IOException
    {
        if (PATH_NAME_TO_WRITER_MAP.containsKey( absPathName ))
        {
            return PATH_NAME_TO_WRITER_MAP.get( absPathName );
        }
        FileWriter fileWriter = new FileWriter( absPathName,
                                                true );
        BufferedWriter bufferedWriter = new BufferedWriter( fileWriter );
        PATH_NAME_TO_WRITER_MAP.put(absPathName, bufferedWriter);
        return bufferedWriter;
    }

    /**
     * Close all of the writers in the map.
     */
    public static void flushAndCloseAllWriters()
    {
        for (Entry<String, BufferedWriter> entry : PATH_NAME_TO_WRITER_MAP.entrySet())
        {
            try
            {
                entry.getValue().flush();
                entry.getValue().close();
            }
            catch ( IOException e )
            {
                LOGGER.warn( "Failed to flush and close pairs file, " + entry.getKey() + ".");
            }
        }
        PATH_NAME_TO_WRITER_MAP.clear();
    }
}
