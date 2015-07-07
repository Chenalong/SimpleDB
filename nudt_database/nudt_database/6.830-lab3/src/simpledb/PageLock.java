package simpledb;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import com.google.common.collect.*;

public class PageLock 
{

	//R keys ,C keys Mapped values
	private Table<TransactionId, PageId, Permissions>			TranAndPidToPerm;
	
	private Table<TransactionId, TransactionId, Set<PageId>>	TranAndTranToPid;
	private Map<PageId, Condition> PidToCond;
	private ReentrantLock rLock;
	
	public PageLock()
	{
		// TODO Auto-generated constructor stub

		TranAndPidToPerm = HashBasedTable.create();
		TranAndTranToPid = HashBasedTable.create();
		rLock = new ReentrantLock();
		PidToCond = new HashMap<PageId, Condition>();
	}
	
	public void lock(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException
	{
		rLock.lock(); 
		System.out.println(tid.toString() +  " " + pid.toString());
		try
		{
			Permissions current_perm = GetCurrentPerm(pid);
			if (current_perm == null)
				GrantLock(tid, pid, perm);
			else if (GetLockCount(pid) == 1 && IsLockedByTran(tid, pid))
				GrantLock(tid, pid, perm);
			else if (IsSharedLock(current_perm) && IsSharedLock(perm))
				GrantLock(tid, pid, Permissions.READ_ONLY);
			else
			{
				AbortIfDeadLock(tid, pid, perm);
				AddDependicies(tid, pid, perm);
				if (IsSharedLock(perm))
				{
					// Block as a reader
					while (!CanGrantSharedLock(tid, pid))
					{
						await(pid);
					}
					GrantLock(tid, pid, Permissions.READ_ONLY);
				}
				else
				{
					// Block as a writer
					while (!CanGrantExclusiveLock(tid, pid))
					{
						await(pid);
						AbortIfDeadLock(tid, pid, perm);
						AddDependicies(tid, pid, perm);
					}
					GrantLock(tid, pid, Permissions.READ_WRITE);
				}
			}
		}
		finally
		{
			rLock.unlock();
		}
	}
	public Permissions GetCurrentPerm(PageId pid)
	{
		Map<TransactionId, Permissions> tp = TranAndPidToPerm.column(pid);
		if(tp.isEmpty())
			return null;
		return tp.values().iterator().next();
	}
	
	public void GrantLock(TransactionId tid,PageId pid,Permissions perm)
	{
		Permissions currentPerm = TranAndPidToPerm.get(tid, pid);
		if(currentPerm == Permissions.READ_WRITE)
			return ;
		//if(tid == null)
			//System.out.println("it is impossible");
		
		TranAndPidToPerm.put(tid, pid, perm);
	}
	
	public void RevokeLock(TransactionId tid,PageId pid)
	{
		TranAndPidToPerm.remove(tid, pid);
		RemoveDependicies(tid, pid);
	}
	
	public int GetLockCount(PageId pid)
	{
		return TranAndPidToPerm.column(pid).keySet().size();
	}
	
	public boolean IsLockedByTran(TransactionId tid,PageId pid)
	{
		if(TranAndPidToPerm.get(tid, pid) == null)
			return false;
		return true;
	}
	
	public boolean IsSharedLock(Permissions perm)
	{
		return perm == Permissions.READ_ONLY;
	}
	
	public boolean CanGrantSharedLock(TransactionId tid,PageId pid)
	{
		Permissions currentPermissions = GetCurrentPerm(pid);
		return (currentPermissions == null || IsSharedLock(currentPermissions) ||IsLockedByTran(tid, pid));
	}
	
	public boolean CanGrantExclusiveLock(TransactionId tid,PageId pid)
	{
		Permissions currentPermissions = GetCurrentPerm(pid);
		return currentPermissions == null || (GetLockCount(pid) == 1 && IsLockedByTran(tid, pid));
	}
	
	public void AddDependicies(TransactionId tid,PageId pid,Permissions perm)
	{
		Permissions currentPermissions = GetCurrentPerm(pid); 
		if(perm == Permissions.READ_WRITE || currentPermissions == Permissions.READ_WRITE)
		{
			for(TransactionId tmpTid:TranAndPidToPerm.column(pid).keySet())
			{
				if(!tmpTid.equals(tid))
				{
					Set<PageId>  tmpSet = TranAndTranToPid.get(tid,tmpTid);
					if(tmpSet==null)
						tmpSet = new HashSet<PageId>();
					tmpSet.add(pid);
					TranAndTranToPid.put(tid, tmpTid, tmpSet);
				}
			}
		}
	}
	
	public void RemoveDependicies(TransactionId tid,PageId pid)
	{
		Set<TransactionId> transactions = new HashSet<TransactionId>();
    	transactions.addAll(TranAndTranToPid.row(tid).keySet());
		//Map<TransactionId, Set<PageId> > TranAndPageId = TranAndTranToPid.row(tid);
		for(TransactionId tmpTid : transactions)
		{
			Set<PageId> tmpSet = TranAndTranToPid.get(tid,tmpTid);
			if(tmpSet !=null)
			{
				if(tmpSet.contains(pid))
					tmpSet.remove(pid);
				
				if(tmpSet.isEmpty())
					TranAndTranToPid.remove(tid,tmpTid);
				else 
				{
					TranAndTranToPid.put(tid,tmpTid,tmpSet);
				}
			}
		}
	}
	
	public void AbortIfDeadLock(TransactionId tid,PageId pid,Permissions perm) throws TransactionAbortedException
	{
		Deque<TransactionId> deq = new ArrayDeque<TransactionId>();
		if(perm == Permissions.READ_WRITE || GetCurrentPerm(pid) == Permissions.READ_WRITE)
		{
			deq.addAll(TranAndPidToPerm.column(pid).keySet());
		}
		while(!deq.isEmpty())
		{
			//System.out.println("-------------");
			Set<TransactionId>set;
			set = TranAndTranToPid.row(deq.removeFirst()).keySet();
			if(set !=null)
			{
				
				if(set.contains(tid))
				{
					//System.out.println("-------------");
					unlockAll(tid);
					throw new TransactionAbortedException();
				}
				else
				{
					deq.addAll(set);
				}
			}
			
		}
		//System.out.println("out of Abortdead");
	}
	
	public boolean IsExclusiveLocked(PageId pid)
	{
		Collection<Permissions>set = TranAndPidToPerm.column(pid).values();
		if(set!=null)
		{
			return set.iterator().next() == Permissions.READ_WRITE;
		}
		return false;
	}

	public boolean isLocked(PageId pid)
	{
		Set<TransactionId>set = TranAndPidToPerm.column(pid).keySet();
		if(set.isEmpty())
			return true;
		return false;
	}
	
	public void await(PageId pid) throws TransactionAbortedException
	{
		try
		{
			Condition cond = PidToCond.get(pid);
			if(cond == null)
				cond = rLock.newCondition();
			PidToCond.put(pid, cond);
			cond.await();
		}
		catch (InterruptedException e)
		{
			TransactionAbortedException tae = new TransactionAbortedException();
			tae.initCause(e);
			throw tae;
			// TODO: handle exception
		}
	}
	
	public void unlock(TransactionId tid, PageId pid)
	{
		rLock.lock();
		try
		{
			unlockInternal(tid, pid);
		}
		finally
		{
			rLock.unlock();
		}
	}
	
	public void unlockAll(TransactionId tid)
	{
		rLock.lock();
		try
		{
			Map<PageId, Permissions> pagelocks = new HashMap<PageId, Permissions>();
			pagelocks.putAll(TranAndPidToPerm.row(tid));
			for (PageId pid : pagelocks.keySet())
				unlockInternal(tid, pid);
		}
		finally
		{
			rLock.unlock();
		}
	}
	
	private void unlockInternal(TransactionId tid, PageId pid)
	{
		RevokeLock(tid, pid);
		Condition guard = PidToCond.get(pid);
		if (guard != null)
			guard.signalAll(); 
	}
	
	public boolean holdsLock(TransactionId tid, PageId pid)
	{
		rLock.lock();
		try
		{
			return TranAndPidToPerm.get(tid, pid) != null;
		}
		finally
		{
			rLock.unlock();
		}
	}
	
	public Set<PageId> GetPageIdAccordTran(TransactionId tid)
	{
		return TranAndPidToPerm.row(tid).keySet();
	}
}