package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/*
 * LockManager is responsible for acquiring and releasing locks on behalf of
 * transactions. Implements strict 2PL using page-level locks.
 * 
 * Supports shared (READ_ONLY) & exclusive (READ_WRITE) locks with
 * lock upgrade capability.
 */
public class LockManager {

    private final Map<PageId, Set<TransactionId>> sharedLocks;
    private final Map<PageId, TransactionId> exclusiveLocks;
    private final Map<TransactionId, Set<PageId>> transactionPages;
    private final Map<TransactionId, Set<TransactionId>> waitingFor;

    public LockManager() {
        sharedLocks = new ConcurrentHashMap<>();
        exclusiveLocks = new ConcurrentHashMap<>();
        transactionPages = new ConcurrentHashMap<>();
        waitingFor = new ConcurrentHashMap<>();
    }

    /*
     * Acquire a lock on behalf of a transaction. If the lock cannot be granted
     * immediately, the thread blocks and waits. If a deadlock timeout expires,
     * throws TransactionAbortedException.
     */
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException {

        synchronized (this) {
            while (true) {
                if (perm == Permissions.READ_ONLY) {
                    // Exclusive lock covers read permission
                    if (hasExclusiveLock(tid, pid)) {
                        clearWaitingEdges(tid);
                        trackLock(tid, pid);
                        return;
                    }
                    // Already hold shared lock
                    if (hasSharedLock(tid, pid)) {
                        clearWaitingEdges(tid);
                        return;
                    }
                    // Grant shared if no exclusive holder exists
                    if (!exclusiveLocks.containsKey(pid) || hasExclusiveLock(tid, pid)) {
                        grantShared(tid, pid);
                        clearWaitingEdges(tid);
                        return;
                    }
                } else {
                    // READ_WRITE (exclusive)
                    if (hasExclusiveLock(tid, pid)) {
                        clearWaitingEdges(tid);
                        return;
                    }
                    // retrieve current shared holders for the page
                    Set<TransactionId> sharedHolders = sharedLocks.getOrDefault(pid, Collections.emptySet());
                    // check if exlucisve lock is already held by another transaction
                    boolean noExclusive = !exclusiveLocks.containsKey(pid);
                    // Can grant exclusive if no exclusive holder and either no shared holders
                    // or only this transaction holds shared locks (upgrade case)
                    boolean canUpgradeOrGrant = noExclusive && (sharedHolders.isEmpty() || (sharedHolders.size() == 1 && sharedHolders.contains(tid)));

                    if (canUpgradeOrGrant) {
                        removeShared(tid, pid);
                        grantExclusive(tid, pid);
                        clearWaitingEdges(tid);
                        return;
                    }
                }

                // updated deadlock detection
                Set<TransactionId> blockers = blockersForRequest(tid, pid, perm);
                // update wait-for graph 
                setWaitingEdges(tid, blockers);
                // cycle detection. Check for deadlock before sleeping on this thread
                if (hasCycle(tid)) {
                    // System.out.println("Deadlock transaction " + tid + " on page " + pid);
                    clearWaitingEdges(tid);
                    throw new TransactionAbortedException();
                }

                try {
                    // sleep & release ownership of LockManager object
                    wait();
                } catch (InterruptedException e) {
                    // System.out.println("Thread interrupted while waiting for lock: " + e.getMessage());
                    clearWaitingEdges(tid);
                    // above method
                    throw new TransactionAbortedException();
                }
            }
        }
    }

    
    // Release a lock on a page held by a transaction.
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        removeShared(tid, pid);
        if (hasExclusiveLock(tid, pid)) {
            exclusiveLocks.remove(pid);
        }
        Set<PageId> held = transactionPages.get(tid);
        if (held != null) {
            held.remove(pid);
        }
        removeWaitingEdgesFor(tid);
        notifyAll();
    }

    // release all locks held by a tid
    public synchronized void releaseAllLocks(TransactionId tid) {
        Set<PageId> held = transactionPages.remove(tid); // full set of pages held by this transaction
        if (held != null) {
            // if that page set exists, iterate through and remove the transaction from shared and exclusive locks
            for (PageId pid : held) {
                Set<TransactionId> s = sharedLocks.get(pid);
                if (s != null) {
                    s.remove(tid);
                    if (s.isEmpty()) sharedLocks.remove(pid);
                }
                if (hasExclusiveLock(tid, pid)) {
                    exclusiveLocks.remove(pid);
                }
            }
        }
        removeWaitingEdgesFor(tid);
        notifyAll();
    }

    // check if transaction holds a lock on a page (either shared or exclusive)
    // synchronized is basically wrapping the whole method in synchronized (this) { ... }
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        return hasSharedLock(tid, pid) || hasExclusiveLock(tid, pid);
    }

    // Private helper methods (called within synchronized blocks)
    private boolean hasSharedLock(TransactionId tid, PageId pid) {
        Set<TransactionId> holders = sharedLocks.get(pid);
        return holders != null && holders.contains(tid);
    }


    private boolean hasExclusiveLock(TransactionId tid, PageId pid) {
        return tid.equals(exclusiveLocks.get(pid));
}

    private void grantShared(TransactionId tid, PageId pid) {
        // check if pid has a set in sharedLocks, if ABSENT, compute (new HashSet) and add it to the map,
        // if present, return the existing set. Then add tid to that set.
        sharedLocks.computeIfAbsent(pid, k -> Collections.synchronizedSet(new HashSet<>())).add(tid);
        trackLock(tid, pid);
    }

    private void grantExclusive(TransactionId tid, PageId pid) {
        exclusiveLocks.put(pid, tid);
        trackLock(tid, pid);
    }

    private void removeShared(TransactionId tid, PageId pid) {
        Set<TransactionId> holders = sharedLocks.get(pid);
        if (holders != null) {
            holders.remove(tid);
            // cleanup stuff
            if (holders.isEmpty()) sharedLocks.remove(pid);
        }
    }

    private void trackLock(TransactionId tid, PageId pid) {
        // track the page in the transaction's held pages set
        // needed for releaseAllLocks to know which pages to release
        transactionPages.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
    }

    // all transactions preventing current tid from getting lock on pid;
    private Set<TransactionId> blockersForRequest(TransactionId tid, PageId pid, Permissions perm) {
        Set<TransactionId> blockers = new HashSet<>();
        TransactionId exclusiveHolder = exclusiveLocks.get(pid);
        // read lock only blocked by other write lock READ-ONLY
        if (exclusiveHolder != null && !exclusiveHolder.equals(tid)) { blockers.add(exclusiveHolder); }
        // write lock blocked by all othere locks 
        if (perm == Permissions.READ_WRITE) {
            Set<TransactionId> sharedHolders = sharedLocks.get(pid);
            if (sharedHolders != null) {
                for (TransactionId holder : sharedHolders) {
                    if (!holder.equals(tid)) { blockers.add(holder); }
                }
            }
        }
        return blockers;
    }

    private void setWaitingEdges(TransactionId tid, Set<TransactionId> blockers) {
        if (blockers.isEmpty()) {
            // tid not waiting for any other transaction, remove from map
            waitingFor.remove(tid);
            return;
        }
        // tid updated to be waiting for provided blockers
        waitingFor.put(tid, new HashSet<>(blockers));
        // System.out.println(blockers);
    }

    // remove tid from wait for map (clearWaitingEdges) and remove it from any other transaction's waiting set
    // normal operations
    private void removeWaitingEdgesFor(TransactionId tid) {
        waitingFor.remove(tid);
        for (Set<TransactionId> blockers : waitingFor.values()) { blockers.remove(tid); }
    }

    // check if there's a cycle in graph
    private boolean hasCycle(TransactionId startTid) {
        // DFS for cycle deteection
        Set<TransactionId> visited = new HashSet<>();
        Deque<TransactionId> stack = new ArrayDeque<>();

        stack.push(startTid);
        while (!stack.isEmpty()) {
            TransactionId currentTid = stack.pop();
            Set<TransactionId> next = waitingFor.get(currentTid);
            if (next == null || next.isEmpty()) { continue; }

            for (TransactionId blocker : next) {
                // System.out.println("Transaction " + startTid + " waiting for " + blocker);
                if (blocker.equals(startTid)) { return true; }
                if (visited.add(blocker)) { stack.push(blocker); }
            }
        }
        return false;
    }

    // remove tid from wait for map
    // remove stale waitfor edges when transaction successfully gets lock or deadlocked
    private void clearWaitingEdges(TransactionId tid) { waitingFor.remove(tid); }
}
