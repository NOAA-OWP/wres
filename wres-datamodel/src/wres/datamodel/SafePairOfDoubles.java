package wres.datamodel;

public class SafePairOfDoubles implements PairOfDoubles
{
    private final double itemOne;
    private final double itemTwo;

    private SafePairOfDoubles(double itemOne, double itemTwo)
    {
        this.itemOne = itemOne;
        this.itemTwo = itemTwo;
    }

    public static SafePairOfDoubles of(double itemOne, double itemTwo)
    {
        return new SafePairOfDoubles(itemOne, itemTwo);
    }

    @Override
    public double getItemOne()
    {
        return itemOne;
    }

    @Override
    public double getItemTwo()
    {
        return itemTwo;
    }

    @Override
    public int compareTo(PairOfDoubles other)
    {
        if (Double.compare(this.getItemOne(), other.getItemOne()) == 0)
        {
            if (Double.compare(this.getItemTwo(), other.getItemTwo()) == 0)
            {
                return 0;
            }
            else if (this.getItemTwo() < other.getItemTwo())
            {
                return -1;
            }
            else
            {
                return 1;
            }
        }
        else if (this.getItemOne() < other.getItemOne())
        {
            return -1;
        }
        else
        {
            return 1;
        }
    }
}
