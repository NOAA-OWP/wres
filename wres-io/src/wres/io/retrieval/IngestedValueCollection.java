package wres.io.retrieval;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.project.Project;
import wres.io.utilities.DataProvider;
import wres.io.utilities.NoDataException;
import wres.util.Collections;

/**
 * Organizes sets of values that were previously ingested and recently retrieved so
 * that they may be later pivotted for scaling operations
 */
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

    /**
     * Determines the first valid step for pivoting valid data
     * <p>
     * TODO: Form a collection of precalculated pivotted values
     * rather than giving the calling information about how to start iterating
     * </p>
     * @throws NoDataException when no data is found
     */
    int getFirstPivotStep( int period, int frequency, int minimumLead)
    {
        if (this.size() == 0)
        {
            throw new NoDataException( "There is no data to pivot" );
        }

        int firstCondensingStep = 0;

        if (this.size() > 1)
        {
            // Get the first value x such that the the xth scaling frequency
            // after the first lead is fully contained within the collection
            for (int x = 0; x < this.size(); ++x)
            {
                int scale = this.getValueDistance();

                // If the first lead of the data is 1, the lead of the first
                // block to pivot will be 1 + the xth iteration of the
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

                // We can pivot values starting at this step if there is at
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


    /**
     * Creates a grouping of pivotted value arrays based on the collected values and the requested shape
     * @param pivotStep The number of the pivot grouping that is needed (pivotStep 6 will
     *                  later data than pivotStep 1)
     * @param period The range of leads that need to be grouped
     * @param frequency The range of leads between pivotted values. If we use a frequency
     *                  of 6 hours, that means the first values in pivotStep 2 will come
     *                  6 hours after the first values in pivotStep 1.
     * @param minimumLead The earliest allowable lead for the entire collection of generated
     *                    pivotted value groups
     * @throws NoDataException when this is empty.
     */
    PivottedValues pivot( final int pivotStep, final int period, final int frequency, final int minimumLead)
    {
        if (this.size() == 0)
        {
            throw new NoDataException( "There is no data to pivot" );
        }

        // Will form a mapping of ensemble positions to each value that will need to be combined underneath it.
        Map<PivottedValues.EnsemblePosition, List<Double>> valueMapping = new TreeMap<>();

        // If there is only one value, format it and return the result
        if (this.size() == 1 && pivotStep == 0)
        {
            for (int valueIndex = 0; valueIndex < this.first().length(); ++valueIndex)
            {
                PivottedValues.EnsemblePosition ensemblePosition = new PivottedValues.EnsemblePosition( valueIndex, this.first().getMemberID( valueIndex ) );
                if (!valueMapping.containsKey( ensemblePosition ))
                {
                    valueMapping.put(ensemblePosition, new ArrayList<>(  ));
                }

                valueMapping.get(ensemblePosition).add(this.first().get(valueIndex));
            }

            return new PivottedValues( this.first().getValidTime(),
                                       this.first().getLead(),
                                       valueMapping );
        }
        else if (this.size() == 1)
        {
            // Since we've already condensed that first value down, we know
            // we're done because there's no other values to combine
            return null;
        }

        // The range of leads that will need to be combined at a time
        final int condensingScale = this.getValueDistance();

        // Say we have values spanning the leads 6, 12, 18, 24, 30, and 36
        // Our condensing scale says we need to scale 18 hours at a time
        // Our frequency is 6 hours; i.e. we need to stagger our condensed values by 6 hours
        //    - This means we need to organize: [6, 12, 18], [12, 18, 24], [18, 24, 30], and [24, 30, 36]
        //    - [30, 36] isn't valid because it doesn't span those 18 hours

        // First determine what would ideally be the first lead in our pivotted set
        //     - Say we are on step 1 (condensingStep is 0)
        //     - Our first stored lead is 6. Since it is the first step, we want to start with 6
        //        o The frequency staggers us by 6 hours, but, since we are on step 1, we stay at 6 since
        //          the frequency shouldn't be accounted for yet
        final int firstBlockLead = this.first().getLead() + frequency * pivotStep;


        // Next, we determine what our exclusive minimum should be
        //    - If we (for some reason) retrieved values below our configured minimum,
        //      we need to adjust for our minimum. If our minimum were 6, we don't want to
        //      combine anything including that six.
        //        o This would eventually shift our condensing range to (6, 24]
        //    - Otherwise, we want to set our exclusive bound respecting our ideal first value
        //        o This would eventually yield (0, 18]
        final int earliestLead = Math.max(minimumLead, firstBlockLead - condensingScale);

        // Next, we form our right bound based on what we wanted our inclusive left bound to be.
        //    - With out example inclusive left bound being 6, we want to take that, add our period of
        //      time that we want to combine (18 hours), then compensate for the distance between values
        //        o Our example from above would yield: (6 + 18) => 24 => (24 - 6) => 18 as our right
        //          inclusive bound
        //    - If we were on our second step and our left inclusive bound were 12, we'd end up with our
        //      right inclusive being:
        //        o (12 + 18) => 30 => (30 - 6) => 24 as our right inclusive bound
        final int lastBlockLead = firstBlockLead + period - condensingScale;

        // If our period is the same as the distance between values, we don't actually need to combine values
        final boolean scalingNotNecessary = condensingScale == period;

        Instant lastValidTime = null;
        int lastLead = -1;

        PivottedValues result;

        // Return null since our needed period of values drops below left bound.  If our right inclusive
        // bound were 18, our period were 18, but our left inclusive bound were 6, we wouldn't be able
        // to pivot because we'd only ever be able to find 12 hours worth of data between the leads
        // (6, 18] instead of the required 18.
        if (lastBlockLead - period < earliestLead && !scalingNotNecessary)
        {
            return null;
        }

        // Return null since we don't even have values matching our right bound. Our offsets ensure
        // that the right bound matches a value. This prevents us from trying to overshoot the right
        // bound of our actual data.
        // If we're on step 5, our ideal inclusive right bound (which should match a value) should be
        // 42. In that case, we'd only be able to combine the values at [30, 36]. That won't be an
        // adequate range, so we can't return anything
        if (!Collections.exists( this.values, ingestedValue -> ingestedValue.getLead() == lastBlockLead ))
        {
            return null;
        }

        // Get everything between our left inclusive bound and our right inclusive bounds
        Collection<IngestedValue> subset = Collections.where(
                this.values,
                value -> value.getLead() >= firstBlockLead
                         && value.getLead() <= lastBlockLead
        );

        // Pivot all of the data so each member value is collected with matching members.
        // Say we have:
        //
        //  values[0]: [ 1,  2,  3,  4,  5,  6]
        //  values[1]: [ 7,  8,  9, 10, 11, 12]
        //  values[2]: [13, 14, 15, 16, 17, 18]
        //  values[3]: [19, 20, 21, 22, 23, 24]
        //  values[4]: [25, 26, 27, 28, 29, 30]
        //  values[5]: [31, 32, 33, 34, 35, 36]
        //                (Map A)
        //
        // We'll end up with:
        //
        //          1: [1,  7, 13, 19, 25, 31]
        //          2: [2,  8, 14, 20, 26, 32]
        //          3: [3,  9, 15, 21, 27, 33]
        //          4: [4, 10, 16, 22, 28, 34]
        //          5: [5, 11, 17, 23, 29, 35]
        //          6: [6, 12, 18, 24, 30, 36]
        //                (Map B)
        //
        // The Arrays 1 through 6 will then be operated on separately; only values within
        // the same column in Map A should be operated on together. For example, the values
        // 1 and 36 shouldn't ever end up in the same equation
        for ( IngestedValue value : subset )
        {
            lastValidTime = value.getValidTime();
            lastLead = value.getLead();

            for ( int index = 0; index < value.length(); ++index )
            {
                PivottedValues.EnsemblePosition ensemblePosition = new PivottedValues.EnsemblePosition( index, value.getMemberID( index ) );
                Pair<Integer, Integer> keyIndex = Pair.of(index, value.getMemberID( index ));
                Integer key = value.getMemberID( index );
                if ( !valueMapping.containsKey( ensemblePosition ) )
                {
                    valueMapping.put( ensemblePosition, new ArrayList<>() );
                }

                valueMapping.get( ensemblePosition ).add( value.get( index ) );
            }
        }
        result = new PivottedValues( lastValidTime,
                                     lastLead,
                                     valueMapping );

        return result;
    }

    /**
     * Evaluates and returns the most common distance between values
     * <p>
     *     If we have the leads: [6, 12, 18, 24, 30, 36], this will tell us to expect 6 hours
     *     between values
     * </p>
     * <p>
     *     If we have the leads: [6, 12, 15, 18, 24, 30, 36], this will ensure that the weird
     *     value won't throw off future calculations and leave us with partial calculations
     * </p>
     * @return The most common distance between values
     * @throws NoDataException when this is empty.
     */
    private int getValueDistance()
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
        }
        else
        {
            LOGGER.error( "The value {} could not be added to the collection of values to evaluate.", value );
        }
    }

    void add( DataProvider row, Project project ) throws SQLException
    {
        IngestedValue value = new IngestedValue( row, project );
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
