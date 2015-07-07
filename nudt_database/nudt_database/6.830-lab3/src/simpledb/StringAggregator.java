package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.activation.FileDataSource;

import org.omg.CORBA.TypeCodeHolder;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator
{
	
	/**
	 * Aggregate constructor
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or
	 *            NO_GROUPING if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., Type.INT_TYPE), or null
	 *            if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException
	 *             if what != COUNT
	 */
	private int					gbfield;
	private int					afield;
	private Type				gbfieldType;
	Op							what;
	TupleDesc					td;
	int							num;
	Map<Field, Integer>	value;
	
	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what)
	{
		// some code goes here
		this.gbfield = gbfield;
		this.gbfieldType = gbfieldtype;
		this.afield = afield;
		this.what = what;
		if (what != Op.COUNT)
			throw new IllegalArgumentException();
		else
		{
			
			if (gbfield == Aggregator.NO_GROUPING)
			{
				td = new TupleDesc(new Type[] { Type.INT_TYPE });
				num = 0;
			}
			else
			{
				td = new TupleDesc(new Type[] { gbfieldType, Type.INT_TYPE });
				value = new HashMap<Field, Integer>();
			}
		}
		
	}
	
	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	public void merge(Tuple tup)
	{
		// some code goes here
		if (gbfield == Aggregator.NO_GROUPING)
		{
			num++;
		}
		else
		{
			Field sf = tup.getField(gbfield);
			if (value.containsKey(sf))
			{
				value.put(sf, value.get(sf) + 1);
			}
			else
			{
				value.put(sf, 1);
			}
		}
	}
	
	public class StringNoGroup extends AbstractDbIterator
	{
		StringAggregator	sa;
		
		public StringNoGroup(StringAggregator sa)
		{
			this.sa = sa;
		}
		
		boolean	iterated	= false;
		
		@Override
		public void open() throws DbException, TransactionAbortedException
		{
			// TODO Auto-generated method stub
			iterated = true;
		}
		
		@Override
		public void rewind() throws DbException, TransactionAbortedException
		{
			// TODO Auto-generated method stub
			open();
		}
		
		@Override
		public TupleDesc getTupleDesc()
		{
			// TODO Auto-generated method stub
			return td;
		}
		
		@Override
		protected Tuple readNext() throws DbException,
				TransactionAbortedException
		{
			// TODO Auto-generated method stub
			if (iterated)
			{
				iterated = false;
				Tuple tp = new Tuple(td);
				tp.setField(0, new IntField(num));
				return tp;
			}
			return null;
		}
		
	}
	
	public class StringGroup extends AbstractDbIterator
	{
		StringAggregator			sa;
		Iterator<Field>	iterated;
		
		public StringGroup(StringAggregator sa)
		{
			this.sa = sa;
		}
		
		@Override
		public void open() throws DbException, TransactionAbortedException
		{
			// TODO Auto-generated method stub
			iterated = sa.value.keySet().iterator();
		}
		
		@Override
		public void rewind() throws DbException, TransactionAbortedException
		{
			// TODO Auto-generated method stub
			open();
		}
		
		@Override
		public TupleDesc getTupleDesc()
		{
			// TODO Auto-generated method stub
			return td;
		}
		
		@Override
		protected Tuple readNext() throws DbException,
				TransactionAbortedException
		{
			// TODO Auto-generated method stub
			if (iterated.hasNext())
			{
				Field filed = iterated.next();
				Tuple tp = new Tuple(td);
				tp.setField(0, filed);
				tp.setField(1, new IntField(value.get(filed)));
				return tp;
			}
			return null;
		}
		
	}
	
	/**
	 * Create a DbIterator over group aggregate results.
	 *
	 * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
	 *         if using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in
	 *         the constructor.
	 */
	public DbIterator iterator()
	{
		if (gbfield == Aggregator.NO_GROUPING)
			return new StringNoGroup(this);
		else
			return new StringGroup(this);
		
		// some code goes here
		// throw new UnsupportedOperationException("implement me");
	}
	
}

