package wres.io.writing.commaseparated.pairs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.metadata.TimeScale;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.io.writing.WriteException;

/**
 * Class for writing {@link TimeSeriesOfSingleValuedPairs}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class SingleValuedPairsWriter extends PairsWriter<TimeSeriesOfSingleValuedPairs>
{

    /**
     * Builds a {@link SingleValuedPairsWriter} incrementally.
     */

    static class SingleValuedPairsWriterBuilder
            extends PairsWriterBuilder<TimeSeriesOfSingleValuedPairs>
    {

        @Override
        public SingleValuedPairsWriter build()
        {
            return new SingleValuedPairsWriter( this );
        }

    }

    /**
     * Write the pairs.
     * 
     * @param pairs the pairs to write
     * @throws NullPointerException if the input is null or required metadata is null
     * @throws WriteException if the writing fails
     */

    @Override
    public void accept( TimeSeriesOfSingleValuedPairs pairs )
    {
        Objects.requireNonNull( pairs, "Cannot write null pairs." );

        Objects.requireNonNull( pairs.getMetadata().getIdentifier(),
                                "Cannot write pairs with a null dataset identifier." );

        Objects.requireNonNull( pairs.getMetadata().getIdentifier().getGeospatialID(),
                                "Cannot write pairs with a null geospatial identifier." );

        try
        {
            // Write header
            this.writeHeaderIfRequired( pairs );

            // Write contents if available
            if ( !pairs.getRawData().isEmpty() )
            {
                LOGGER.debug( "Writing pairs for {} to {}", pairs.getMetadata().getIdentifier(), this.getPath() );

                // Feature to write, which is fixed across all pairs
                String featureName = pairs.getMetadata().getIdentifier().getGeospatialID().getLocationName();

                // Prepare
                this.getWriteLock().lock();
                LOGGER.trace( "Acquired pair writing lock on {}", this.getPath() );

                // Write by appending
                try ( BufferedWriter writer = this.getBufferedWriter( true ) )
                {
                    // Iterate in time-series order
                    for ( TimeSeries<SingleValuedPair> nextSeries : pairs.basisTimeIterator() )
                    {

                        Instant basisTime = nextSeries.getEarliestBasisTime();

                        for ( Event<SingleValuedPair> nextPair : nextSeries.timeIterator() )
                        {

                            StringJoiner joiner = new StringJoiner( PairsWriter.DELIMITER );

                            // Move to next line
                            writer.write( System.lineSeparator() );

                            // Feature description
                            joiner.add( featureName );

                            // ISO8601 datetime string
                            joiner.add( nextPair.getTime().toString() );

                            // Lead duration in standard units
                            joiner.add( Long.toString( Duration.between( basisTime, nextPair.getTime() )
                                                               .get( this.getTimeResolution() ) ) );

                            // Format left and right values
                            if ( Objects.nonNull( this.getDecimalFormatter() ) )
                            {
                                joiner.add( this.getDecimalFormatter().format( nextPair.getValue().getLeft() ) );
                                joiner.add( this.getDecimalFormatter().format( nextPair.getValue().getRight() ) );
                            }
                            // No format
                            else
                            {
                                joiner.add( Double.toString( nextPair.getValue().getLeft() ) );
                                joiner.add( Double.toString( nextPair.getValue().getRight() ) );
                            }

                            // Write next line
                            writer.write( joiner.toString() );

                        }
                    }

                    LOGGER.trace( "{} pairs written to {}.", pairs.getRawData().size(), this.getPath() );
                }
                // Clean-up
                finally
                {
                    this.getWriteLock().unlock();
                    LOGGER.trace( "Released pair writing lock on {}", this.getPath() );
                }
            }
            else
            {
                LOGGER.debug( "No pairs written to {} as the pairs were empty.", this.getPath() );
            }
        }
        catch ( IOException e )
        {
            throw new WriteException( "Unable to write pairs.", e );
        }
    }

    @Override
    String getHeaderFromPairs( TimeSeriesOfSingleValuedPairs pairs )
    {
        Objects.requireNonNull( pairs, "Cannot obtain header from null pairs." );

        StringJoiner joiner = new StringJoiner( "," );

        joiner.add( "FEATURE DESCRIPTION" )
              .add( "VALID TIME" );

        if ( pairs.getMetadata().hasTimeScale() )
        {
            TimeScale timeScale = pairs.getMetadata().getTimeScale();

            joiner.add( "LEAD DURATION IN " + this.getTimeResolution().toString().toUpperCase()
                        + " ["
                        + timeScale.getFunction()
                        + " OVER PAST "
                        + timeScale.getPeriod().get( this.getTimeResolution() )
                        + " "
                        + this.getTimeResolution().toString().toUpperCase()
                        + "]" );
        }
        else
        {
            joiner.add( "LEAD DURATION IN " + this.getTimeResolution().toString().toUpperCase() );
        }

        joiner.add( "LEFT IN " + pairs.getMetadata().getMeasurementUnit().getUnit() );

        joiner.add( "RIGHT IN " + pairs.getMetadata().getMeasurementUnit().getUnit() );

        return joiner.toString();
    }

    /**
     * Hidden constructor.
     * 
     * @param <T> the type of pairs to write
     * @param builder the builder
     * @throws NullPointerException if any of the expected inputs is null
     */

    private SingleValuedPairsWriter( SingleValuedPairsWriterBuilder builder )
    {
        super( builder );
    }

}
