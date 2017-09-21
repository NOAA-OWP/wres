package wres.io.concurrency;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.util.Strings;

public class PairWriter extends WRESCallable<Boolean>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( PairWriter.class );
    private static final Object PAIR_OUTPUT_LOCK = new Object();
    private static final String OUTPUT_HEADER = "Feature,Date,Window,Left,Right";
    private static final String DELIMITER = ",";
    private static final AtomicBoolean HAS_WRITTEN = new AtomicBoolean();

    @Override
    protected Boolean execute() throws Exception
    {

        Boolean success = false;

        synchronized ( PAIR_OUTPUT_LOCK )
        {
            if ( !this.fileDestination.endsWith( ".csv" ) )
            {
                this.fileDestination += ".csv";
            }

            BufferedWriter writer = null;

            try
            {
                if ( !Files.exists( Paths.get( this.fileDestination ) ) )
                {
                    writer =
                            new BufferedWriter( new FileWriter( this.fileDestination,
                                                                false ) );
                    writer.write( OUTPUT_HEADER );
                    writer.newLine();
                    writer.flush();
                }
                else
                {
                    writer =
                            new BufferedWriter( new FileWriter( this.fileDestination,
                                                                HAS_WRITTEN.get() ) );
                }

                StringJoiner line = new StringJoiner( DELIMITER );
                StringJoiner arrayJoiner = new StringJoiner( DELIMITER );

                line.add( this.featureDescription );
                line.add( this.date );

                // Convert from 0 index to 1 index for easier representation
                // i.e. first window, second, third, ...
                // instead of: zeroth window, first, second, third, ...
                line.add( String.valueOf( this.windowNum + 1 ) );

                if (this.left == null)
                {
                    line.add("");
                }
                else
                {
                    line.add( String.valueOf( this.left ) );
                }

                for ( Double rightValue : this.right )
                {
                    arrayJoiner.add( String.valueOf( rightValue ) );
                }

                line.add( arrayJoiner.toString() );

                writer.write( line.toString() );
                writer.newLine();
                writer.flush();

                HAS_WRITTEN.set( true );

                success = true;
            }
            catch ( IOException error )
            {
                LOGGER.error( Strings.getStackTrace( error ) );
                error.printStackTrace();
            }
            finally
            {
                if ( writer != null )
                {
                    try
                    {
                        writer.close();
                    }
                    catch ( IOException e )
                    {
                        LOGGER.error( Strings.getStackTrace(e) );
                    }
                }
            }
        }

        return success;
    }

    @Override
    protected boolean validate()
    {
        if (!Strings.hasValue( this.fileDestination ))
        {
            throw new IllegalArgumentException(
                    "The PairWriter does not have a destination to write to." );
        }
        else if (!Strings.hasValue( this.featureDescription ))
        {
            throw new IllegalArgumentException(
                    "No feature was specified for where pairs belong to." );
        }
        else if (!Strings.hasValue( this.date ))
        {
            throw new IllegalArgumentException(
                    "No date was specified for when the paired data occurred." );
        }
        return true;
    }

    @Override
    protected Logger getLogger()
    {
        return LOGGER;
    }

    public void setFileDestination(String fileDestination)
    {
        this.fileDestination = fileDestination;
    }

    public void setDate(String date)
    {
        this.date = date;
    }

    public void setFeatureDescription(String featureDescription)
    {
        this.featureDescription = featureDescription;
    }

    public void setWindowNum(int windowNum)
    {
        this.windowNum = windowNum;
    }

    public void setLeft(Double left)
    {
        this.left = left;
    }

    public void setRight(double[] right)
    {
        if (right == null)
        {
            right = new double[0];
        }
        this.right = right;
    }

    private String fileDestination;
    private String date;
    private String featureDescription;
    private int windowNum;
    private Double left;
    private double[] right;
}
