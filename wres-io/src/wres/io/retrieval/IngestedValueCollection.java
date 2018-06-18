package wres.io.retrieval;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.ProjectDetails;
import wres.io.utilities.NoDataException;
import wres.util.Collections;

class IngestedValueCollection
{
    private static final Logger
            LOGGER = LoggerFactory.getLogger( IngestedValueCollection.class );

    private final List<IngestedValue> values;
    private Long reference;
    private int scale = -1;

    IngestedValueCollection()
    {
        this.values = new ArrayList<>(  );
    }

    int getFirstCondensingStep(int period, int frequency, int minimumLead)
            throws NoDataException
    {
        if (this.size() == 0)
        {
            throw new NoDataException( "There is no data to condense" );
        }

        int firstCondensingStep = 0;

        if (this.size() > 1)
        {
            // Get the first value x such that the the xth scaling frequency
            // after the first lead is fully contained within the collection
            for (int x = 0; x < this.size(); ++x)
            {
                int scale = this.getScale();

                // If the first lead of the data is 1, the lead of the first
                // block to condense will be 1 + the xth iteration of the
                // frequency. If the frequency is 3 and we're on our 4th pass,
                // that means that we're looking at the first lead of our block
                // being 1 + 12, or 13.
                int firstBlockLead = this.first().getLead() + frequency * x;

                // The leads are exclusive-inclusive. As a result, we either
                // want the max of the caller overridden minimum lead or that
                // first lead in the block at the beginning of its scale. If
                // the scale is 1 value every 3 hours, that means that we'll want to
                // get the first value within that scale that will land on the
                // lead for our first block.  If the minimum lead was 4 (i.e.
                // the caller doesn't want to start condensing anything prior
                // to lead 4), that means we'll start condensing values upwards
                // of lead 10.
                int earliestLead = Math.max(minimumLead, firstBlockLead - scale);

                // In our block, we want to span the given period. If our period were 6 hours,
                // We want to be 6 hours ahead of where we would naturally
                // start the block (the size of the scale prior to the first
                // value of the block). Our scale is 3 ours and our period is 6;
                // that means that we should be able to get 2 values within our
                // block. Our first value is at 13, so, starting at 10, adding
                // the scale yields our first value (lead = 13), and adding the
                // scale again yields our second value (lead = 16)
                int lastBlockLead = firstBlockLead - scale + period;

                // If the scale is equivalent to the period, no scale operation
                // is needed and we can accept a value that doesn't have a full
                // period within the window
                boolean scalingNotNecessary = scale == period;

                // We can condense values starting at this step if there is at
                // least one lead in the entire collection that matches the end
                // of our block and either we won't be scaling or scaling won't
                // attempt to add values prior to the earliest possible lead
                // for condensing
                boolean canCondense = Collections.exists(
                        this.values,
                        ingestedValue -> ingestedValue.getLead() == lastBlockLead &&
                                         (ingestedValue.getLead() - period >= earliestLead || scalingNotNecessary));

                if (canCondense)
                {
                    // We have determined that our first step for condensing
                    // iterations over our data will be at step x such that
                    // the first lead to combine with others will be the first
                    // lead of the data plus the xth iteration of the frequency
                    firstCondensingStep = x;
                    break;
                }
            }
        }

        return firstCondensingStep;
    }

    CondensedIngestedValue condense(final int condensingStep, final int period, final int frequency, final int minimumLead)
            throws NoDataException
    {
        if (this.size() == 0)
        {
            throw new NoDataException( "There is no data to condense" );
        }

        Map<Integer, List<Double>> valueMapping = new TreeMap<>();

        if (this.size() == 1 && condensingStep == 0)
        {
            for (int valueIndex = 0; valueIndex < this.first().length(); ++valueIndex)
            {
                List<Double> value = new ArrayList<>(  );
                value.add( this.first().get(valueIndex) );
                valueMapping.put( valueIndex, value );
            }

            return new CondensedIngestedValue( this.first().getValidTime(),
                                               this.first().getLead(),
                                               valueMapping );
        }
        else if (this.size() == 1)
        {
            // Since we've already condensed that first value down, we know
            // we're done because there's no other values to combine
            return null;
        }

        final int condensingScale = this.getScale();
        final int firstBlockLead = this.first().getLead() + frequency * condensingStep;
        final int earliestLead = Math.max(minimumLead, firstBlockLead - condensingScale);
        final int lastBlockLead = firstBlockLead + period - condensingScale;
        final boolean scalingNotNecessary = condensingScale == period;

        Instant lastValidTime = null;
        int lastLead = -1;

        CondensedIngestedValue result = null;

        // Checks if the collection contains the value for the last block and
        // it is either fully in the collection or it is already in the correct
        // scale
        boolean canCondense = Collections.exists(
                this.values,
                ingestedValue -> ingestedValue.getLead() == lastBlockLead &&
                                 (ingestedValue.getLead() - period >= earliestLead || scalingNotNecessary));

        List<IngestedValue> subset;

        if ( canCondense )
        {
            subset = Collections.where(
                    this.values,
                    value -> value.getLead() >= firstBlockLead
                             && value.getLead() <= lastBlockLead
            );

            for ( IngestedValue value : subset )
            {
                lastValidTime = value.getValidTime();
                lastLead = value.getLead();

                for ( int index = 0; index < value.length(); ++index )
                {
                    if ( !valueMapping.containsKey( index ) )
                    {
                        valueMapping.put( index, new ArrayList<>() );
                    }

                    valueMapping.get( index ).add( value.get( index ) );
                }
            }
            result = new CondensedIngestedValue( lastValidTime,
                                                 lastLead,
                                                 valueMapping );
        }

        return result;
    }

    private int getScale() throws NoDataException
    {
        if (this.size() == 0)
        {
            throw new NoDataException( "There is no data to interrogate for scale." );
        }

        // There can be no scale if there's only one value
        if (this.scale == -1 && this.size() == 1)
        {
            this.scale = 0;
        }

        // If there's no scale
        if (this.scale == -1)
        {
            Map<Integer, Integer> scaleCount = new HashMap<>();

            // For every contained value after the first
            for ( int index = 1; index < this.size(); ++index )
            {
                // Determine the difference in leads between this value and the previous
                int difference =
                        this.values.get( index ).getLead() - this.values.get(index - 1 ).getLead();

                // If there's no record for this degree of difference, add a new record for it
                if ( !scaleCount.containsKey( difference ) )
                {
                    scaleCount.put( difference, 0 );
                }

                // Increment the number of occurances for the difference
                scaleCount.put( difference, scaleCount.get( difference ) + 1 );
            }

            // Set the scale to the difference that occurred the most times
            // Say you have a scaleCount map like:
            //    1 -> 18
            //    3 -> 2
            //    7 -> 1
            //
            // Here, it looks like there were some erroneous gaps, because
            // there was generally an hour difference between each lead. There
            // was a 3 hour gap in there twice, and there was a 7 hour gap in
            // there once. We want the most common distance, which is 18
            this.scale = Collections.getKeyByValueFunction(
                    scaleCount,
                    ( compare, to ) -> compare == Math.max( compare, to )
            );
        }
        return this.scale;
    }

    private IngestedValue first()
    {
        IngestedValue firstValue = null;

        if (this.size() != 0)
        {
            firstValue = this.values.get( 0 );
        }

        return firstValue;
    }

    int size()
    {
        return values.size();
    }

    void add(IngestedValue value)
    {
        boolean canAdd = false;

        if (this.reference == null && this.size() == 0)
        {
            this.reference = value.getReferenceEpoch();
            canAdd = true;
        }
        else if ( this.reference == null ||
                  (this.reference != null && this.reference.equals(value.getReferenceEpoch())))
        {
            canAdd = true;
        }

        if (canAdd)
        {
            this.values.add( value );
            //this.values.sort( Comparator.naturalOrder() );
        }
        else
        {
            LOGGER.error( "The value {} could not be added to the collection of values to evaluate.", value );
        }
    }

    void add( ResultSet row, ProjectDetails projectDetails) throws SQLException
    {
        IngestedValue value = new IngestedValue( row, projectDetails );
        this.add( value );
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );

        for (IngestedValue value : this.values)
        {
            joiner.add( "[" + value + "]" );
        }

        return joiner.toString();
    }
}
