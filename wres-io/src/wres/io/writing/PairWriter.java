package wres.io.writing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.Feature;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ConfigHelper;

public class PairWriter extends WRESCallable<Boolean>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( PairWriter.class );
    private static final Object PAIR_OUTPUT_LOCK = new Object();
    private static final String OUTPUT_HEADER = "Feature,Date,Window,Left,Right";
    private static final String DELIMITER = ",";
    private static final String PAIR_FILENAME = "/pairs.csv";
    private static final String BASELINE_FILENAME = "/baseline_pairs.csv";

    /** Guarded by PAIR_OUTPUT_LOCK */
    private static boolean headerHasBeenWritten = false;

    /** Guarded by BASELINE_PAIR_OUTPUT_LOCK */
    private static boolean baselineHeaderHasBeenWritten = false;

    private final DestinationConfig destinationConfig;
    private final String date;
    private final Feature feature;
    private final int windowNum;
    private final List<PairOfDoubleAndVectorOfDoubles> pairs;
    private final boolean isBaseline;

    // TODO: This needs to be cleaned up; this is far too many parameters
    // Maybe required setters?
    public PairWriter( DestinationConfig destinationConfig,
                       String date,
                       Feature feature,
                       int windowNum,
                       List<PairOfDoubleAndVectorOfDoubles> pairs,
                       boolean isBaseline)
    {
        this.destinationConfig = destinationConfig;
        this.date = date;
        this.feature = feature;
        this.windowNum = windowNum;
        this.pairs = pairs;
        this.isBaseline = isBaseline;
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

        DecimalFormat formatter = null;
        String configuredFormat = this.getDestinationConfig().getDecimalFormat();

        if ( configuredFormat != null && !configuredFormat.isEmpty() )
        {
            formatter = new DecimalFormat();
            formatter.applyPattern( configuredFormat );
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

                for (PairOfDoubleAndVectorOfDoubles pair : this.getPairs() )
                {
                    StringJoiner line = new StringJoiner( DELIMITER );
                    StringJoiner arrayJoiner = new StringJoiner( DELIMITER );

                    line.add( ConfigHelper.getFeatureDescription( this.getFeature() ) );
                    line.add( this.getDate() );

                    // Convert from 0 index to 1 index for easier representation
                    // i.e. first window, second, third, ...
                    // instead of: zeroth window, first, second, third, ...
                    line.add( String.valueOf( this.getWindowNum() + 1 ) );

                    double left = pair.getItemOne();

                    if ( Double.compare( left, Double.NaN ) == 0 )
                    {
                        line.add( "NaN" );
                    }
                    else if ( formatter != null )
                    {
                        line.add( formatter.format( left ) );
                    }
                    else
                    {
                        line.add( String.valueOf( left ) );
                    }

                    double[] rightValues = pair.getItemTwo();

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

                    line.add( arrayJoiner.toString() );

                    writer.write( line.toString() );
                    writer.newLine();
                }

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

    public List<PairOfDoubleAndVectorOfDoubles> getPairs()
    {
        return this.pairs;
    }
}
