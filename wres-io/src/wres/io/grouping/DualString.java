package wres.io.grouping;

import java.util.Objects;

/**
 * Created by ctubbs on 7/10/17.
 */
public class DualString implements Comparable<DualString>
{

    public DualString(String first, String second)
    {
        this.first = first;
        this.second = second;
    }

    public String getFirst()
    {
        return this.first;
    }

    public String getSecond()
    {
        return this.second;
    }

    private final String first;
    private final String second;

    @Override
    public int compareTo (final DualString other) {
        int comparison = -1;

        if (this.getFirst() == null && other.getFirst() == null)
        {
            comparison = 0;
        }
        else if (this.getFirst() != null && other.getFirst() == null)
        {
            comparison = 1;
        }
        else if (this.getFirst() == null && other.getFirst() != null)
        {
            comparison = -1;
        }
        else
        {
            comparison = this.getFirst().compareToIgnoreCase(other.getFirst());
        }

        if (comparison == 0)
        {
            if (this.getSecond() == null && other.getSecond() == null)
            {
                comparison = 0;
            }
            else if (this.getSecond() != null && other.getSecond() == null)
            {
                comparison = 1;
            }
            else if (this.getSecond() == null && other.getSecond() != null)
            {
                comparison = -1;
            }
            else
            {
                comparison = this.getSecond().compareToIgnoreCase(other.getSecond());
            }
        }


        return comparison;
    }

    @Override
    public boolean equals (final Object obj)
    {
        return obj != null && (obj instanceof  DualString && this.compareTo((DualString)obj) == 0);
    }

    @Override
    public int hashCode () {
        return Objects.hash(this.getFirst(), this.getSecond());
    }
}
