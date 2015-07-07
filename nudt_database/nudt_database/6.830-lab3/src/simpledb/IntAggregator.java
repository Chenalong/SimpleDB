package simpledb;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.activation.FileDataSource;

import simpledb.Predicate.Op;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator
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
	 *            the aggregation operator
	 */
	
	private int gbfiled;
	private int afield;
	private Type gbfiType;
	private TupleDesc td;
	private InternalAggregator ia;
	
	public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what)
	{
		// some code goes here
		this.gbfiled = gbfield;
		this.gbfiType = gbfieldtype;
		this.afield = afield;
		if(gbfield != -1)
		{
			td = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
			ia = new IntGroupAggregator(what);	
		}
		else 
		{
			td = new TupleDesc(new Type[]{Type.INT_TYPE});
			ia = new IntNoGroupAggregator(what);
		}
	}
	
	public TupleDesc getTupleDesc()
	{
		// some code goes here
		return td;
	}
	
	
	
	
	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	public abstract class InternalAggregator
	{
		public int defaulValue;
		Op what;
		public InternalAggregator(Op what)
		{
			this.what = what;
			switch (what)
			{
				case MIN:
					defaulValue = Integer.MAX_VALUE;
					break;
				case MAX:
					defaulValue = Integer.MIN_VALUE;
					break;
				default:
					defaulValue = 0 ;
					break;
			}
		}
		public Integer[] cal(Integer v1,Integer v2,Integer v3,Op what)
		{
			switch (what)
			{
				case AVG:
					v2 += v1;v3++;
					break;
				case MIN:
					v2 = Math.min(v1, v2);
					break;
				case MAX:
					v2 = Math.max(v1, v2);
					break;
				case COUNT:
					v3++;
					break;
				case SUM:
					v2+=v1;
					break;
				default:
					break;
			}
			return new Integer[]{v2,v3};
		}
		public abstract void merge(Tuple tup);
		
		public abstract DbIterator iterator();
	
		
		
	}
	
	public class IntNoGroupAggregator extends InternalAggregator
	{
		int value ,num ;
		Integer[] result;
		IntNoGroupDbIterator ingd;
		public IntNoGroupAggregator(Op what)
		{
			super(what);
			value = defaulValue;
			num = 0;
			ingd = new IntNoGroupDbIterator(this);
		}
		
		@Override
		public void merge(Tuple tup)
		{
			IntField field = (IntField)tup.getField(afield);
			result = cal(field.getValue(), value, num, what);
			value = result[0];
			num = result[1];
		}
		
		@Override
		public DbIterator iterator()
		{
			return ingd;
		}
	}
	
	public class IntGroupAggregator extends InternalAggregator
	{
		Map<Field,Integer>value;
		Map<Field, Integer>num;
		Integer[] result;
		private IntGroupDbIterator igb;
		public IntGroupAggregator(Op what)
		{
			super(what);
			value = new HashMap<Field, Integer>();
			num = new HashMap<Field, Integer>();
			igb = new IntGroupDbIterator(this);
		}
		
		
		public void merge(Tuple tup)
		{
			IntField field = (IntField)tup.getField(afield);
			Field key =tup.getField(gbfiled);
			int tmpValue = value.containsKey(key)?value.get(key):defaulValue;
			int tmpNum = num.containsKey(key)?num.get(key):defaulValue;
			result = cal(field.getValue(),tmpValue, tmpNum, what);
			value.put(key, result[0]);
			num.put(key, result[1]);
		}
		public DbIterator iterator()
		{
			return igb;
		}
	}
	
	public abstract class IntAbstractDbIterator extends AbstractDbIterator
	{
		public int calNext(int v1,int v2,Op what)
		{
			switch (what)
			{
				case MIN:
				case MAX:
				case SUM:
					return v1;
				case COUNT:
					return v2;
				case AVG:
					return v1 / v2;
					//break;
				default:
					throw new IllegalArgumentException("The argument is illegal");
					//break;
			}
		}
	}
	
	public class IntNoGroupDbIterator extends IntAbstractDbIterator
	{
		boolean iterated;
		IntNoGroupAggregator iga;
		public IntNoGroupDbIterator(IntNoGroupAggregator iga)
		{
			this.iga = iga;
		}
		
		
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
			//return null;
			return td;
		}

		@Override
		protected Tuple readNext() throws DbException,
				TransactionAbortedException
		{
			// TODO Auto-generated method stub
			Tuple tp = new Tuple(td);
			if(iterated)
			{
				iterated = false;
				int value = calNext(iga.value, iga.num, iga.what);
				tp.setField(0, new IntField(value));
			
				return tp;
			}
			return null;
		}
		
	}
	
	public class IntGroupDbIterator extends IntAbstractDbIterator
	{
		Iterator<? extends Field> iterated;
		IntGroupAggregator iga;
		public IntGroupDbIterator(IntGroupAggregator iga)
		{
			this.iga = iga;
		}
		
		
		@Override
		public void open() throws DbException, TransactionAbortedException
		{
			// TODO Auto-generated method stub
			iterated = iga.value.keySet().iterator();
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
			//return null;
			return td;
		}

		@Override
		protected Tuple readNext() throws DbException,
				TransactionAbortedException
		{
			// TODO Auto-generated method stub
			Tuple tp = new Tuple(td);
			if(iterated.hasNext())
			{
				Field filed = iterated.next();
				int value = iga.value.get(filed);
				int num  = iga.num.get(filed);
				tp.setField(0, filed);
				tp.setField(1, new IntField(calNext(value, num,iga.what)));
				return tp;
			}
			return null;
		}
		
	}
	public void merge(Tuple tup)
	{
		// some code goes here 
		ia.merge(tup);
		System.err.println("hello-----------");
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
		// some code goes here
		return ia.iterator();
		//throw new UnsupportedOperationException("implement me");
	}
	
}
