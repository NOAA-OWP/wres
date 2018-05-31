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
                int firstBlockLead = this.first().getLead() + frequency * x;
                int earliestLead = Math.max(minimumLead, firstBlockLead - scale);
                int lastBlockLead = firstBlockLead + period - scale;

                // If the scale is equivalent to the period, no scale operation
                // is needed and we can accept a value that doesn't have a full
                // period within the window
                boolean scalingNotNecessary = scale == period;
                boolean canCondense = Collections.exists(
                        this.values,
                        ingestedValue -> ingestedValue.getLead() == lastBlockLead &&
                                         (ingestedValue.getLead() - period >= earliestLead || scalingNotNecessary));

                if (canCondense)
                {
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

        if (this.scale == -1 && this.size() == 1)
        {
            this.scale = 0;
        }

        if (this.scale == -1)
        {
            Map<Integer, Integer> scaleCount = new HashMap<>();

            for ( int index = 1; index < this.size(); ++index )
            {
                int difference =
                        this.values.get( index ).getLead() - this.values.get(index - 1 ).getLead();

                if ( !scaleCount.containsKey( difference ) )
                {
                    scaleCount.put( difference, 0 );
                }

                scaleCount.put( difference, scaleCount.get( difference ) + 1 );
            }

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
