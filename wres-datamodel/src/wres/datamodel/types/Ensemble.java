package wres.datamodel.types;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicReference;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Stores an array of ensemble member values as double values and, optionally, an array of ensemble member labels.
 *
 * @author James Brown
 */
public class Ensemble implements Comparable<Ensemble>
{
    /** Ensemble labels. */
    public static class Labels
    {
        /** A cache of ensemble names to re-use, indexed by the concatenation of the names as a unique identifier.
         * Allow one hundred; there should not be more than a handful of instances across recent evaluations. */
        private static final Cache<String, Labels> LABELS_CACHE = Caffeine.newBuilder()
                                                                          .maximumSize( 100 )
                                                                          .build();

        /** Empty labels. */
        private static final Labels EMPTY_LABELS = new Labels( new String[0] );

        /** The labels, which may be empty. */
        private final String[] labs;

        /**
         * @param labels the labels
         * @return an instance for the input labels
         */

        public static Labels of( String... labels )
        {
            return Labels.getFromCache( labels );
        }

        /**
         * @return an instance with no labels defined
         */

        public static Labels of()
        {
            return Labels.EMPTY_LABELS;
        }

        /**
         * @return a clone of the labels.
         */

        public String[] getLabels()
        {
            return this.labs.clone();
        }

        /**
         * @param ensembleName the ensemble name
         * @return {@code true} if the prescribed label is present, otherwise {@code false}.
         * @throws NullPointerException if the ensembleName is null.
         */

        public boolean hasLabel( String ensembleName )
        {
            Objects.requireNonNull( ensembleName );

            // Labels are de-duplicated, so search the unordered labels, else would need to store de-duplicated ordered
            // labels to use a search that relies on order
            for ( String next : labs )
            {
                if ( ensembleName.equals( next ) )
                {
                    return true;
                }
            }

            return false;
        }

        @Override
        public boolean equals( Object other )
        {
            if ( other == this )
            {
                return true;
            }

            if ( !( other instanceof Labels otherLabels ) )
            {
                return false;
            }

            return Arrays.equals( this.labs, otherLabels.labs );
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode( this.labs );
        }

        /**
         * @return a set of labels from the cache
         */

        private static Labels getFromCache( String[] labels )
        {
            if ( Objects.isNull( labels ) || labels.length == 0 )
            {
                return Labels.EMPTY_LABELS;
            }

            String id = String.join( ",", labels );

            Labels cached = Labels.LABELS_CACHE.getIfPresent( id );

            if ( Objects.nonNull( cached ) )
            {
                return cached;
            }

            // Add to cache
            cached = new Labels( labels );
            Labels.LABELS_CACHE.put( id, cached );

            return cached;
        }

        /**
         * Hidden constructor
         * @param labels the labels
         */

        private Labels( String[] labels )
        {
            Objects.requireNonNull( labels );

            this.labs = labels.clone();
        }
    }

    /** The ensemble members. */
    private final double[] members;

    /** The member labels. */
    private final Labels labels;

    /** The cached, sorted ensemble members. */
    private final AtomicReference<double[]> sorted = new AtomicReference<>();

    /** Whether the sorted members should be cached, trading cpu for memory. */
    private final boolean cacheSorted;

    /**
     * Returns a {@link Ensemble} from a primitive array of members.
     *
     * @param members the ensemble members
     * @return the ensemble
     */

    public static Ensemble of( final double... members )
    {
        return new Ensemble( members,
                             Labels.EMPTY_LABELS,
                             false );
    }

    /**
     * Returns a {@link Ensemble} from a primitive array of members and a corresponding array of member labels. The
     * members are ordered according to the order of the labels.
     *
     * @param members the ensemble members
     * @param labels the labels
     * @return the ensemble
     * @throws IllegalArgumentException if the members and labels have different size
     * @throws NullPointerException if the members are null
     */

    public static Ensemble of( final double[] members,
                               Labels labels )
    {
        return new Ensemble( members, labels, false );
    }

    /**
     * Returns a {@link Ensemble} from a primitive array of members and a corresponding array of member labels. The
     * members are ordered according to the order of the labels.
     *
     * @param members the ensemble members
     * @param labels the labels
     * @param cacheSorted whether to cache the sorted ensemble members or generate them on-the-fly
     * @return the ensemble
     * @throws IllegalArgumentException if the members and labels have different size
     * @throws NullPointerException if the members are null
     */

    public static Ensemble of( final double[] members,
                               Labels labels,
                               boolean cacheSorted )
    {
        return new Ensemble( members, labels, cacheSorted );
    }

    /**
     * Returns a copy of the ensemble members.
     *
     * @return the members
     */

    public double[] getMembers()
    {
        return this.members.clone();
    }

    /**
     * <p>Sorts the ensemble members, either sorting on demand or by returning the previously cached result.
     *
     * <p>TODO: a possible optimization or rebalancing of the trade-off between cpu and memory would involve caching the
     * indexes of the sorted members as a short array, rather than the sorted members themselves. This would for
     * sorting of ensembles with up to 32,767 members before resorting to on-the-fly calculation (far in excess of the
     * practical number of members that could be evaluated, anyway).
     *
     * @return the sorted ensemble members
     */

    public double[] getSortedMembers()
    {
        // No caching? Then compute the sorted members
        if ( !this.cacheSorted )
        {
            double[] result = this.getMembers(); // Deep copy
            Arrays.sort( result );
            return result;
        }

        double[] result = this.sorted.get();

        // Already cached? Then return a deep copy
        if ( Objects.nonNull( result ) )
        {
            return result.clone();
        }

        result = this.getMembers(); // Deep copy
        Arrays.sort( result );

        // May sort across multiple threads, but only one wins and sets the cache. Avoids blocking.
        if ( !this.sorted.compareAndSet( null, result ) )
        {
            result = this.sorted.get()
                                .clone();
        }

        return result;
    }

    /**
     * @return whether the sorted ensemble members should be cached.
     */

    public boolean areSortedMembersCached()
    {
        return this.cacheSorted;
    }

    /**
     * Returns a member that corresponds to a prescribed label.
     *
     * @param label the label
     * @return the ensemble member
     * @throws IllegalArgumentException if the label is not present
     * @throws NullPointerException if the input is null
     */

    public double getMember( String label )
    {
        Objects.requireNonNull( label );

        for ( int i = 0; i < this.labels.labs.length; i++ )
        {
            if ( label.equals( this.labels.labs[i] ) )
            {
                return this.members[i];
            }
        }

        throw new IllegalArgumentException( "Unrecognized label '" + label + "'." );
    }

    /**
     * @return {@code true} if one or more labels is defined, {@code false} if no labels are defined.
     */

    public boolean hasLabels()
    {
        return this.labels.labs.length > 0;
    }

    /**
     * Returns the labels.
     *
     * @return the labels
     */

    public Labels getLabels()
    {
        return this.labels;
    }

    /**
     * Return the number of members present.
     *
     * @return the number of members
     */

    public int size()
    {
        return members.length;
    }

    @Override
    public int compareTo( Ensemble other )
    {
        int returnMe = Arrays.compare( this.getMembers(), other.getMembers() );

        if ( returnMe != 0 )
        {
            return returnMe;
        }

        returnMe = Boolean.compare( this.hasLabels(), other.hasLabels() );

        if ( returnMe != 0 )
        {
            return returnMe;
        }

        if ( this.hasLabels() )
        {
            Labels labs = this.getLabels();
            Labels otherLabs = other.getLabels();

            Comparator<String[]> compare = Comparator.nullsFirst( Arrays::compare );

            return Objects.compare( labs.labs, otherLabs.labs, compare );
        }

        return 0;
    }

    @Override
    public boolean equals( Object other )
    {
        if ( !( other instanceof Ensemble otherVec ) )
        {
            return false;
        }

        // Label status?
        if ( !this.getLabels()
                  .equals( otherVec.getLabels() ) )
        {
            return false;
        }

        return Arrays.equals( this.getMembers(), otherVec.getMembers() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.labels,
                             Arrays.hashCode( this.getMembers() ) );
    }

    @Override
    public String toString()
    {
        if ( this.hasLabels() )
        {
            StringJoiner joiner = new StringJoiner( ",", "[", "]" );

            for ( int i = 0; i < this.members.length; i++ )
            {
                joiner.add( "{" + this.labels.labs[i] + "," + this.members[i] + "}" );
            }

            return joiner.toString();
        }
        else
        {
            return Arrays.toString( this.getMembers() ).replace( " ", "" );
        }
    }

    /**
     * Hidden constructor.
     *
     * @param members the ensemble members
     * @param labels the ensemble member labels
     * @param cacheSorted whether to cache the sorted ensemble members or generate them on-the-fly
     * @throws IllegalArgumentException if the labels are non-null and differ in size to the number of members
     * @throws NullPointerException if the members are null
     */

    private Ensemble( final double[] members, Labels labels, boolean cacheSorted )
    {
        Objects.requireNonNull( members );

        if( Objects.isNull( labels ) )
        {
            labels = Labels.EMPTY_LABELS;
        }

        if ( labels != Labels.EMPTY_LABELS && members.length != labels.labs.length )
        {
            throw new IllegalArgumentException( "Expected the same number of members (" + members.length
                                                + ") as labels ("
                                                + labels.labs.length
                                                + ")." );
        }

        this.labels = labels;

        // Deep copy the members
        this.members = members.clone();
        this.cacheSorted = cacheSorted;
    }

}
