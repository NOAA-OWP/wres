package wres.io.concurrency;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.Feature;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.io.config.ConfigHelper;

public class PairWriter extends WRESCallable<Boolean>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( PairWriter.class );
    private static final Object PAIR_OUTPUT_LOCK = new Object();
    private static final String OUTPUT_HEADER = "Feature,Date,Window,Left,Right";
    private static final String DELIMITER = ",";

    /** Guarded by PAIR_OUTPUT_LOCK */
    private static boolean headerHasBeenWritten = false;

    private final DestinationConfig destinationConfig;
    private final String date;
    private final Feature feature;
    private final int windowNum;
    private final PairOfDoubleAndVectorOfDoubles pair;

    public PairWriter( DestinationConfig destinationConfig,
                       String date,
                       Feature feature,
                       int windowNum,
                       PairOfDoubleAndVectorOfDoubles pair )
    {
        this.destinationConfig = destinationConfig;
        this.date = date;
        this.feature = feature;
        this.windowNum = windowNum;
        this.pair = pair;
    }

    @Override
    protected Boolean execute() throws IOException, ProjectConfigException
    {

        boolean success = false;

        File directoryFromDestinationConfig =
                ConfigHelper.getDirectoryFromDestinationConfig( this.getDestinationConfig() );

        String actualFileDestination = directoryFromDestinationConfig.getCanonicalPath()
                + "/pairs.csv";

        DecimalFormat formatter = null;
        String configuredFormat = this.getDestinationConfig().getDecimalFormat();

        if ( configuredFormat != null && !configuredFormat.isEmpty() )
        {
            formatter = new DecimalFormat();
            formatter.applyPattern( configuredFormat );
        }

        synchronized ( PAIR_OUTPUT_LOCK )
        {
            if ( !PairWriter.headerHasBeenWritten )
            {
                Files.deleteIfExists( Paths.get( actualFileDestination) );
            }

            try ( FileWriter fileWriter = new FileWriter( actualFileDestination,
                                                         true );
                  BufferedWriter writer = new BufferedWriter( fileWriter ) )
            {
                if ( !PairWriter.headerHasBeenWritten )
                {
                    writer.write( OUTPUT_HEADER );
                    writer.newLine();

                    PairWriter.headerHasBeenWritten = true;
                }

                StringJoiner line = new StringJoiner( DELIMITER );
                StringJoiner arrayJoiner = new StringJoiner( DELIMITER );

                line.add( ConfigHelper.getFeatureDescription( this.getFeature() ) );
                line.add( this.getDate() );

                // Convert from 0 index to 1 index for easier representation
                // i.e. first window, second, third, ...
                // instead of: zeroth window, first, second, third, ...
                line.add( String.valueOf( this.getWindowNum() + 1 ) );

                double left = this.getPair().getItemOne();

                if (left == Double.NaN)
                {
                    line.add("NaN");
                }
                else if ( formatter != null )
                {
                    line.add( formatter.format( left ) );
                }
                else
                {
                    line.add( String.valueOf( left ) );
                }

                double[] rightValues = this.getPair().getItemTwo();

                Arrays.sort( rightValues );

                for ( Double rightValue : rightValues )
                {
                    if (rightValue == Double.NaN)
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

                line.add( arrayJoiner.toString() );

                writer.write( line.toString() );
                writer.newLine();

                success = true;
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

    public DestinationConfig getDestinationConfig()
    {
        return this.destinationConfig;
    }

    public String getDate()
    {
        return this.date;
    }

    public Feature getFeature()
    {
        return this.feature;
    }

    public int getWindowNum()
    {
        return this.windowNum;
    }

    public PairOfDoubleAndVectorOfDoubles getPair()
    {
        return this.pair;
    }
}
