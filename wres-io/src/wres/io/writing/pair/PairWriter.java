package wres.io.writing.pair;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DestinationConfig;
import wres.config.generated.Feature;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.io.concurrency.WRESRunnableException;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.util.CalculationException;

/**
 * Returns a string to be written to a pairs file.
 */
public class PairWriter implements Supplier<Pair<Path,String>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( PairWriter.class );
    private static final String NEWLINE = System.lineSeparator();
    private static final String DELIMITER = ",";
    private static final String PAIR_FILENAME = "/pairs.csv";
    private static final String BASELINE_FILENAME = "/baseline_pairs.csv";

    private final DestinationConfig destinationConfig;
    private final Instant date;
    private final Feature feature;
    private final int windowNum;
    private final EnsemblePair pair;
    private final boolean isBaseline;
    private final int poolingStep;
    private final ProjectDetails projectDetails;
    private final int lead;

    private DecimalFormat formatter;

    public static class Builder
    {
        private final DestinationConfig destinationConfig;
        private final Instant date;
        private final Feature feature;
        private final int leadIteration;
        private final EnsemblePair pair;
        private final boolean isBaseline;
        private final int poolingStep;
        private final ProjectDetails projectDetails;
        private final int lead;

        public Builder()
        {
            this.destinationConfig = null;
            this.date = null;
            this.feature = null;
            this.leadIteration = Integer.MIN_VALUE;
            this.pair = null;
            this.isBaseline = false;
            this.poolingStep = Integer.MIN_VALUE;
            this.projectDetails = null;
            this.lead = 0;
        }

        private Builder( DestinationConfig destinationConfig,
                         Instant date,
                         Feature feature,
                         Integer windowNum,
                         EnsemblePair pair,
                         Boolean isBaseline,
                         Integer poolingStep,
                         ProjectDetails projectDetails,
                         Integer lead)
        {
            this.destinationConfig = destinationConfig;
            this.date = date;
            this.feature = feature;
            this.leadIteration = windowNum;
            this.pair = pair;
            this.isBaseline = isBaseline;
            this.poolingStep = poolingStep;
            this.projectDetails = projectDetails;
            this.lead = lead;
        }

        public Builder setDestinationConfig(DestinationConfig destinationConfig)
        {
            return new Builder( destinationConfig,
                                this.date,
                                this.feature,
                                this.leadIteration,
                                this.pair,
                                this.isBaseline,
                                this.poolingStep,
                                this.projectDetails,
                                this.lead);
        }

        public Builder setDate(Instant date)
        {
            return new Builder( this.destinationConfig,
                                date,
                                this.feature,
                                this.leadIteration,
                                this.pair,
                                this.isBaseline,
                                this.poolingStep,
                                this.projectDetails,
                                this.lead);
        }

        public Builder setFeature(Feature feature)
        {
            return new Builder( this.destinationConfig,
                                this.date,
                                feature,
                                this.leadIteration,
                                this.pair,
                                this.isBaseline,
                                this.poolingStep,
                                this.projectDetails,
                                this.lead);
        }

        public Builder setLeadIteration(Integer leadIteration)
        {
            return new Builder( this.destinationConfig,
                                this.date,
                                this.feature,
                                leadIteration,
                                this.pair,
                                this.isBaseline,
                                this.poolingStep,
                                this.projectDetails,
                                this.lead);
        }

        public Builder setPair(EnsemblePair pair)
        {
            return new Builder( this.destinationConfig,
                                this.date,
                                this.feature,
                                this.leadIteration,
                                pair,
                                this.isBaseline,
                                this.poolingStep,
                                this.projectDetails,
                                this.lead);
        }

        public Builder setIsBaseline(boolean isBaseline)
        {
            return new Builder( this.destinationConfig,
                                this.date,
                                this.feature,
                                this.leadIteration,
                                this.pair,
                                isBaseline,
                                this.poolingStep,
                                this.projectDetails,
                                this.lead);
        }

        public Builder setPoolingStep(Integer poolingStep)
        {
            return new Builder( this.destinationConfig,
                                this.date,
                                this.feature,
                                this.leadIteration,
                                this.pair,
                                this.isBaseline,
                                poolingStep,
                                this.projectDetails,
                                this.lead);
        }

        public Builder setProjectDetails(ProjectDetails projectDetails)
        {
            return new Builder( this.destinationConfig,
                                this.date,
                                this.feature,
                                this.leadIteration,
                                this.pair,
                                this.isBaseline,
                                this.poolingStep,
                                projectDetails,
                                this.lead);
        }

        public Builder setLead(Integer lead)
        {
            return new Builder( this.destinationConfig,
                                this.date,
                                this.feature,
                                this.leadIteration,
                                this.pair,
                                this.isBaseline,
                                this.poolingStep,
                                this.projectDetails,
                                lead);
        }

        public PairWriter build()
        {
            int errorCount = 0;
            StringJoiner errorJoiner = new StringJoiner( NEWLINE );

            if (this.destinationConfig == null)
            {
                errorCount += 1;
                errorJoiner.add( "There was no destination passed to write to.");
            }

            if (this.date == null)
            {
                errorCount += 1;
                errorJoiner.add("No date was added to record.");
            }

            if (feature == null)
            {
                errorCount += 1;
                errorJoiner.add("No feature was added to record.");
            }

            if (this.leadIteration == Integer.MIN_VALUE)
            {
                errorCount += 1;
                errorJoiner.add("The iteration was not added to record.");
            }

            if (this.pair == null)
            {
                errorCount += 1;
                errorJoiner.add("No pair was added to record.");
            }

            if (this.poolingStep == Integer.MIN_VALUE)
            {
                errorCount += 1;
                errorJoiner.add("No pooling step was configured.");
            }

            if (this.projectDetails == null)
            {
                errorCount += 1;
                errorJoiner.add("No details about the project were passed.");
            }

            if (errorCount > 0)
            {
                throw new IllegalArgumentException( "A PairWriter could not be "
                                                    + "created: " +
                                                    errorJoiner.toString() );
            }

            return new PairWriter( this.destinationConfig,
                                   this.date,
                                   this.feature,
                                   this.leadIteration,
                                   this.pair,
                                   this.isBaseline,
                                   this.poolingStep,
                                   this.projectDetails,
                                   this.lead );
        }
    }

    private PairWriter( DestinationConfig destinationConfig,
                        Instant date,
                        Feature feature,
                        int windowNum,
                        EnsemblePair pair,
                        boolean isBaseline,
                        int poolingStep,
                        ProjectDetails projectDetails,
                        int lead )
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
    public Pair<Path,String> get()
    {
        File directoryFromDestinationConfig =
                ConfigHelper.getDirectoryFromDestinationConfig( this.getDestinationConfig() );

        String actualFileDestination = directoryFromDestinationConfig.toString();

        try
        {
            actualFileDestination =
                    directoryFromDestinationConfig.getCanonicalPath();
        }
        catch ( IOException ioe )
        {
            // Not critical to get the full path, keep the original and keep going.
            LOGGER.warn( "Could not get canonical path for {}",
                         directoryFromDestinationConfig );
        }

        if (this.isBaseline)
        {
            actualFileDestination += BASELINE_FILENAME;
        }
        else
        {
            actualFileDestination += PAIR_FILENAME;
        }

        Path destination = Paths.get( actualFileDestination );

        StringJoiner line = new StringJoiner( DELIMITER );

        line.add( ConfigHelper.getFeatureDescription( this.getFeature() ) );

        // Avoid changing date format to iso format because benchmarks
        line.add( this.date.toString()
                           .replace( "T", " " )
                           .replace( "Z", "" ) );

        // But above could be as simple as this (and be more precise):
        //line.add( this.date.toString() );

        line.add(String.valueOf(this.lead));

        try
        {
            line.add( this.getWindow() );
        }
        catch ( CalculationException e )
        {
            throw new WRESRunnableException( "Pairs could not be gotten for " +
                                             ConfigHelper.getFeatureDescription( this.feature ),
                                             e );
        }

        line.add(this.getLeftValue());
        line.add(this.getRightValues());

        String toWrite = line.toString();

        return Pair.of( destination, toWrite );
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

    private String getWindow() throws CalculationException
    {

        int window = this.getWindowNum();

        // If basis time pooling is used, you get intermediary pools. This means
        // that you don't just get entries for window 0, 1, 2, 3, 4, etc, you
        // get window 0 pooling step 1, window 0 pooling step 2, window 1
        // pooling step 1, etc. To find the overall window (i.e. "this is the
        // fifth calculation"), you need to break down the calculation to
        // compensate for the number of intermediate windows
        if ( this.projectDetails.getPairingMode() == ProjectDetails.PairingMode.ROLLING )
        {
            window *= (this.projectDetails.getIssuePoolCount( this.feature ));
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

        double leftValue = pair.getLeft();
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
        double[] rightValues = pair.getRight();
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
