package wres.io.writing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.InvalidPropertiesFormatException;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.Feature;
import wres.config.generated.TimeAggregationMode;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.util.TimeHelper;

public class PairWriter extends WRESCallable<Boolean>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( PairWriter.class );
    private static final Object PAIR_OUTPUT_LOCK = new Object();
    private static final String OUTPUT_HEADER = "Feature,Date,Lead,Window,Left,Right";
    private static final String DELIMITER = ",";
    private static final String PAIR_FILENAME = "/pairs.csv";
    private static final String BASELINE_FILENAME = "/baseline_pairs.csv";

    // TODO: Actually guard this static variable
    /** Guarded by PAIR_OUTPUT_LOCK */
    private static boolean headerHasBeenWritten = false;

    // TODO: Actually guard this static variable
    /** Guarded by BASELINE_PAIR_OUTPUT_LOCK */
    private static boolean baselineHeaderHasBeenWritten = false;

    private final DestinationConfig destinationConfig;
    private final String date;
    private final Feature feature;
    private final int windowNum;
    private final PairOfDoubleAndVectorOfDoubles pair;
    private final boolean isBaseline;
    private final int sequenceStep;
    private final ProjectDetails projectDetails;

    private DecimalFormat formatter;

    // TODO: IMPLEMENT BUILDER
    public PairWriter( DestinationConfig destinationConfig,
                       String date,
                       Feature feature,
                       int windowNum,
                       PairOfDoubleAndVectorOfDoubles pair,
                       boolean isBaseline,
                       int sequenceStep,
                       ProjectDetails projectDetails)
    {
        this.destinationConfig = destinationConfig;
        this.date = date;
        this.feature = feature;
        this.windowNum = windowNum;
        this.pair = pair;
        this.isBaseline = isBaseline;
        this.sequenceStep = sequenceStep;
        this.projectDetails = projectDetails;
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

            try ( FileWriter fileWriter = new FileWriter( actualFileDestination,
                                                         true );
                  BufferedWriter writer = new BufferedWriter( fileWriter ) )
            {
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
                line.add( this.getPairingDate() );

                line.add( String.valueOf( this.getLeadHour() ) );

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

    private String getPairingDate() throws InvalidPropertiesFormatException
    {
        String pairingDate;

        if (this.projectDetails.getAggregation().getMode() == TimeAggregationMode.ROLLING)
        {
            // We need to derive the basis time from the lead, and date
            pairingDate = TimeHelper.minus( date, "hour", this.getLeadHour() );
        }
        else
        {
            pairingDate = date;
        }

        return pairingDate;
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
        else if ( this.getDate() == null || this.getDate().isEmpty() )
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

    private String getDate()
    {
        return this.date;
    }

    private double getLeadHour() throws InvalidPropertiesFormatException
    {
        // This defines back-to-back. This doesn't work for rolling.
        // Given a 4 hour period, back-to-back would yield 4 then 8,
        // but so will rolling, which should be 4 then 5
        double lead;

        if (this.projectDetails.getAggregation().getMode() == TimeAggregationMode.ROLLING)
        {
            lead = this.getWindowNum() +
                   TimeHelper.unitsToHours( this.projectDetails.getAggregationUnit(),
                                            this.projectDetails.getAggregationPeriod() );
        }
        else
        {
            lead = TimeHelper.unitsToHours( this.projectDetails.getAggregationUnit(),
                                            this.projectDetails.getAggregationPeriod() ) *
                   ( this.getWindowNum() + 1 );
        }

        return lead;
    }

    private Feature getFeature()
    {
        return this.feature;
    }

    private String getWindow()
            throws SQLException, InvalidPropertiesFormatException
    {

        int window = this.getWindowNum();

        if (this.projectDetails.getAggregation().getMode() == TimeAggregationMode.ROLLING)
        {
            // This doesn't quite work. When rolling over to the next
            // lead, it stays at the largest value prior. For instance,
            // if the number goes from 1 through 5, the next window for
            // the next lead will then be 5.
            window *= (this.projectDetails.getRollingWindowCount( this.feature ) + 1);
            window += this.sequenceStep;
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
}
