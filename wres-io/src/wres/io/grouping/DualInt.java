package wres.io.grouping;

/**
 * Represents a grouping of two ints
 */
public class DualInt implements Comparable<DualInt>, Cloneable {

    public DualInt(int one, int two)
    {
        this.first = one;
        this.second = two;
    }

    public int getFirst()
    {
        return this.first;
    }

    public int getSecond()
    {
        return this.second;
    }

    private final int first;
    private final int second;

    @Override
    public int compareTo (DualInt other) {
        int equality = 0;

        if (this.getFirst() > other.getFirst())
        {
            equality = 1;
        }
        else if (this.getFirst() < other.getFirst())
        {
            equality = -1;
        }

        if (equality == 0 && this.getSecond() > other.getSecond())
        {
            equality = 1;
        }
        else if (equality == 0 && this.getSecond() < other.getSecond())
        {
            equality = -1;
        }

        return equality;
    }

    @Override
    public boolean equals(Object obj) {
        boolean equal = false;

        if (obj instanceof DualInt)
        {
            DualInt other = (DualInt)obj;
            equal = this.getFirst() == other.getFirst() && this.getSecond() == other.getSecond();
        }

        return equal;
    }

    @Override
    public int hashCode() {
        return this.getFirst() ^ this.getSecond();
    }

    @Override
    protected Object clone () throws CloneNotSupportedException {
        return new DualInt(this.getFirst(), this.getSecond());
    }
}
