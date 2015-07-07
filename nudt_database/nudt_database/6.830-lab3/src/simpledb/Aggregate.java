package simpledb;

import java.util.*;

import org.omg.CORBA.TypeCodeHolder;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends AbstractDbIterator
{
	
	/**
	 * Constructor.
	 *
	 * Implementation hint: depending on the type of afield, you will want to
	 * construct an IntAggregator or StringAggregator to help you with your
	 * implementation of readNext().
	 * 
	 *
	 * @param child
	 *            The DbIterator that is feeding us tuples.
	 * @param afield
	 *            The column over which we are computing an aggregate.
	 * @param gfield
	 *            The column over which we are grouping the result, or -1 if
	 *            there is no grouping
	 * @param aop
	 *            The aggregation operator to use
	 */
	private Aggregator.Op aop;
	private DbIterator child;
	private int afield;
	private int gfield;
	private static final int NO_GROUPING = -1;
	private Aggregator ag;
	private TupleDesc td;
	AbstractDbIterator di;
	public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop)
	{
		this.afield = afield;
		this.gfield = gfield;
		this.child = child;
		this.aop = aop;
		Type aTp = child.getTupleDesc().getType(afield);
		String aTpString = child.getTupleDesc().getFieldName(afield);
		String gTpString = getAggNameString();
		Type gTp = null;
		if(gfield != -1)
		{
			gTp = child.getTupleDesc().getType(gfield);
			td = new TupleDesc(new Type[]{gTp,aTp},new String[]{gTpString,aTpString});
		}
		else 
		{
			td = new TupleDesc(new Type[]{aTp},new String[]{aTpString});
		}
		
		if(aTp == Type.INT_TYPE)
			ag = new IntAggregator(gfield,gTp,afield,aop);
		else 
			ag = new StringAggregator(gfield, gTp, afield, aop);
		di = (AbstractDbIterator)ag.iterator();
		
	}
	
	public static String aggName(Aggregator.Op aop)
	{
		switch (aop)
		{
			case MIN:
				return "min";
			case MAX:
				return "max";
			case AVG:
				return "avg";
			case SUM:
				return "sum";
			case COUNT:
				return "count";
		}
		return "";
	}
	
	public void open() throws NoSuchElementException, DbException,
			TransactionAbortedException
	{
		// some code goes here
		child.open();
		while(child.hasNext())
		{
			ag.merge(child.next());
		}
		di.open();
	}
	
	/**
	 * Returns the next tuple. If there is a group by field, then the first
	 * field is the field by which we are grouping, and the second field is the
	 * result of computing the aggregate, If there is no group by field, then
	 * the result tuple should contain one field representing the result of the
	 * aggregate. Should return null if there are no more tuples.
	 */
	protected Tuple readNext() throws TransactionAbortedException, DbException
	{
		// some code goes here
		return di.readNext();
	}
	
	public void rewind() throws DbException, TransactionAbortedException
	{
		// some code goes here
		di.rewind();
	}
	
	/**
	 * Returns the TupleDesc of this Aggregate. If there is no group by field,
	 * this will have one field - the aggregate column. If there is a group by
	 * field, the first field will be the group by field, and the second will be
	 * the aggregate value column.
	 * 
	 * The name of an aggregate column should be informative. For example:
	 * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
	 * given in the constructor, and child_td is the TupleDesc of the child
	 * iterator.
	 */
	
	public String getAggNameString()
	{
		return String.format("aggName(%s) (%s)",aggName(aop),child.getTupleDesc().getFieldName(afield));
	}
	public TupleDesc getTupleDesc()
	{
		// some code goes here
		return td;
	}
	
	public void close()
	{
		// some code goes here
		di.close();
		child.close();
	}
}
