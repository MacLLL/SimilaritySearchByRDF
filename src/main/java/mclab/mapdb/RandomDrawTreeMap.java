package mclab.mapdb;


import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import mclab.deploy.LSHServer;
import mclab.lsh.DefaultHasher;
import mclab.lsh.Hasher;
import mclab.lsh.LocalitySensitiveHasher;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class RandomDrawTreeMap<K,V>
        extends AbstractMap<K,V>
        implements ConcurrentMap<K,V>,Closeable {
    protected static final Logger LOG = Logger.getLogger(HTreeMap.class.getName());

    /**
     * Lu's comment
     * the largest nodes under a same slot
     */
    public int BUCKET_OVERFLOW = 4;
    public int BUCKET_LENGTH = 28;

    protected int DIRECTORY_NODE_SIZE = 0;
    protected int NUM_BITS_PER_COMPARISON = 0;
    protected int BITS_COMPARISON_MASK = 0;
    protected int BITMAP_SIZE = 0;
    protected int MAX_TREE_LEVEL = 0;
    protected int TOTAL_HASH_LENGTH = 0;

    public int SEG = 0;

    /**
     * is this a Map or Set?  if false, entries do not have values, only keys are allowed
     */
    protected final boolean hasValues;

    /**
     * Salt added to hash before rehashing, so it is harder to trigger hash collision attack.
     */
    protected final int hashSalt;

    /**
     * serializer for key
     */
    protected final Serializer<K> keySerializer;
    /**
     * serializer for value
     */
    protected final Serializer<V> valueSerializer;

    /**
     * engines is the Map for every different engine, which keep different sub-index
     * Integer---->sub-index-ID, Engine---->index structure, where keep the data as (recid,binary form)
     */
    public final ConcurrentHashMap<Integer, Engine> engines = new ConcurrentHashMap<>();

    /**
     * Snapshot for different sub-index
     */
    protected final ConcurrentHashMap<Integer, Engine> snapshots = new ConcurrentHashMap<>();

    /**
     * Indicate whether the engine is closed
     */
    protected final boolean closeEngine;

    /**
     * Defalut value creater(used when there is no such key in the Map)
     */
    protected final Fun.Function1<V, K> valueCreator;

    /**
     * RAM storage threshold
     */
    protected final long ramThreshold;

    /**
     * controlling the simulation of default MapDB, indicate whether this Map is locked
     */
    protected ReentrantReadWriteLock initStorageLock = new ReentrantReadWriteLock();

    /**
     * It indicate whether certain sub-index is initalized
     */
    protected boolean defaultMapDBInitialized[]= null;

    /**
     * Indicates if this collection collection was not made by DB by user.
     * If user can not access DB object, we must shutdown Executor and close Engine ourself
     * in close() method.
     */
    protected final boolean closeExecutor;

    public final ExecutorService executor;

    /**
     * locks for each sub-index
     */
    protected HashMap<Integer, ReentrantReadWriteLock> structureLocks =
            new HashMap<Integer, ReentrantReadWriteLock>();
    /**
     * Counters for each sub-index, each Integer corresponds to each sub-index, long[] for the segments of each sub-index
     */
    protected final ConcurrentHashMap<Integer, Long[]> counterRecids =
            new ConcurrentHashMap<Integer, Long[]>();

    /**
     * each sub-index corresponding to a long array which save the recid, recid is used to get the data from engine.
     * here we suppose long[16], each long number for each segment
     */
    protected final ConcurrentHashMap<Integer, Long[]> partitionRootRec = new ConcurrentHashMap();

    /**
     * locks for each segments in each partition
     */
    protected final ConcurrentHashMap<Integer, ReentrantReadWriteLock[]> partitionRamLock =
            new ConcurrentHashMap<Integer, ReentrantReadWriteLock[]>();

    /**
     * persist SSD lock for each partition
     */
    protected final ConcurrentHashMap<Integer, ReentrantReadWriteLock[]> partitionPersistLock =
            new ConcurrentHashMap<Integer, ReentrantReadWriteLock[]>();
    /**
     * partitioner:include salt hash and LSH hash, more partition strategies need to be discovered
     */
    public Partitioner<K> partitioner;

    private final String hasherName;
    public final Hasher hasher;

    private final int tableId;
    /**
     * For persistant index
     */
    private final String workingDirectory;
    private final String name;

    /**
     * node which holds key-value pair
     */
    protected static final class LinkedNode<K, V> {
        public final long next;
        public final K key;
        public final V value;
        public LinkedNode(final long next, final K key, final V value) {
            if (CC.ASSERT && next >>> 48 != 0)
                throw new DBException.DataCorruption("next recid too big");
            this.key = key;
            this.value = value;
            this.next = next;
        }
    }

    /**
     * only for test(means can be deleted)
     * @return no mean
     */
    public void getAllRootRecIdForEachSegment(){
        for (int i = 0; i < partitionRootRec.size(); i++) {
            System.out.println("partiion #" + i +"has" + partitionRootRec.get(i) );
            for (long x: partitionRootRec.get(i)) {
                System.out.print(x+",");
            }
            System.out.println();
        }
    }

    /**
     * Serializer for LinkedNode, which keep the data nodes in our indexing structure
     */
    protected final Serializer<RandomDrawTreeMap.LinkedNode<K, V>> LN_SERIALIZER =
            new Serializer<RandomDrawTreeMap.LinkedNode<K, V>>() {
        /** used to check that every 64000 th element has consistent has before and after (de)serialization*/
        int serCounter = 0;
        @Override
        public void serialize(DataOutput out, RandomDrawTreeMap.LinkedNode<K, V> value) throws IOException {
            if (((serCounter++) & 0xFFFF) == 0) {
                assertHashConsistent(value.key);
            }
            DataIO.packLong(out, value.next);
            keySerializer.serialize(out, value.key);
            if (hasValues) {
                valueSerializer.serialize(out, value.value);
            }
        }
        @Override
        public RandomDrawTreeMap.LinkedNode<K, V> deserialize(DataInput in, int available) throws IOException {
            if (CC.ASSERT && available == 0)
                throw new AssertionError();
            return new RandomDrawTreeMap.LinkedNode<K, V>(
                    DataIO.unpackLong(in),
                    keySerializer.deserialize(in, -1),
                    //here we find for No serialized value, we treat the value default as Boolean
                    //so when we put the key into this map, we put the pair like (key, boolean)
                    //this is the reason why we put (key,true) in hashTable in the Test
                    hasValues ? valueSerializer.deserialize(in, -1) : (V) Boolean.TRUE
            );
        }
        @Override
        public boolean isTrusted() {
            return keySerializer.isTrusted() && valueSerializer.isTrusted();
        }
    };

    /**
     * To check the consistent of the key-value pairs before and after serialize and deserialize
     * @param key
     * @throws IOException
     */
    private final void assertHashConsistent(K key) throws IOException {
        int hash = keySerializer.hashCode(key);
        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        keySerializer.serialize(out, key);
        DataIO.DataInputByteArray in = new DataIO.DataInputByteArray(out.buf, 0);
        K key2 = keySerializer.deserialize(in, -1);
        if (hash != keySerializer.hashCode(key2)) {
            throw new IllegalArgumentException(
                    "Key does not have consistent hash before and after deserialization. Class: " +
                            key.getClass());
        }
        if (!keySerializer.equals(key, key2)) {
            throw new IllegalArgumentException(
                    "Key does not have consistent equals before and after deserialization. Class: " +
                            key.getClass());
        }
        if (out.pos != in.pos) {
            throw new IllegalArgumentException("Key has inconsistent serialization length. Class: " +
                    key.getClass());
        }
    }

    /**
     * Serializer for Dir[4], which keeps the BitMap for directory node in indexing structure
     */
    protected final Serializer<Object> DIR_SERIALIZER = new Serializer<Object>() {
        @Override
        // dir is a int[], the first 4 is 4 int as bitmap, then the long new recid
        // Notes: Also, the last bit in recid indicates whether it is a k-node(1) or d-node(0)
        public void serialize(DataOutput out, Object value) throws IOException {
            DataIO.DataOutputByteArray out2 = (DataIO.DataOutputByteArray) out;
            if (value instanceof long[]) {
                serializeLong(out2, value);
                return;
            }
            int[] c = (int[]) value;
            if (CC.ASSERT) {
                //4 is the bitmap, Integer.bitCount(c[0]) to Integer.bitCount(c[3]) are counting the real
                // data
                int totalDataBits = 0;
                for (int i = 0; i < BITMAP_SIZE; i++) {
                    totalDataBits += Integer.bitCount(c[i]);
                }
                int len = BITMAP_SIZE + totalDataBits;
                if (len != c.length)
                    throw new DBException.DataCorruption("bitmap!=len, bitmap:" + len + ", len:" + c.length +
                            ", BITMAP_SIZE:" + BITMAP_SIZE);
            }
            //write bitmaps
            for (int i = 0; i < BITMAP_SIZE; i++) {
                out2.writeInt(c[i]);
            }
            if (c.length == BITMAP_SIZE) {
                return;
            }
            //TODO: Bug here, we don't check the all dir node
            out2.packLong((((long) c[BITMAP_SIZE]) << 1) | 1L); //save the k-node
            for (int i = BITMAP_SIZE + 1; i < c.length; i++) {
                out2.packLong(c[i]);
            }
        }
        //TODO:implement long, we plan to do it in the future
        private void serializeLong(DataIO.DataOutputByteArray out, Object value) throws IOException {
      /*
      long[] c = (long[]) value;

      if (CC.ASSERT) {
        int len = 2 +
                Long.bitCount(c[0]) +
                Long.bitCount(c[1]);

        if (len != c.length)
          throw new DBException.DataCorruption("bitmap!=len");
      }

      out.writeLong(c[0]);
      out.writeLong(c[1]);
      if (c.length == 2)
        return;

      out.packLong(c[2] << 1);
      for (int i = 3; i < c.length; i++) {
        out.packLong(c[i]);
      }*/
            throw new IOException("RandomDrawTreeMap does not support serializeLong for now");
        }

        @Override
        public Object deserialize(DataInput in, int available) throws IOException {
            DataIO.DataInputInternal in2 = (DataIO.DataInputInternal) in;
            //length of dir is l longs, each long has 6 bytes (not 8)
            //to save memory zero values are skipped,
            //there is bitmap at first 16 bytes, each non-zero long has bit set
            //to determine offset one must traverse bitmap and count number of bits set
            int[] bitmaps = new int[BITMAP_SIZE];
            int len = 0;
            for (int i = 0; i < BITMAP_SIZE; i++) {
                bitmaps[i] = in.readInt();
                len += Integer.bitCount(bitmaps[i]);
            }
            if (len == 0) {
                return new int[BITMAP_SIZE];
            }
            long firstVal = in2.unpackLong();
            //return int[]
            int[] ret = new int[BITMAP_SIZE + len];
            for (int i = 0; i < BITMAP_SIZE; i++) {
                ret[i] = bitmaps[i];
            }
            ret[BITMAP_SIZE] = (int) (firstVal >>> 1);// keep the same as serialize
            len += BITMAP_SIZE;
            //unpack the all int after BITMAP_SIZE+1 until len
            in2.unpackIntArray(ret, BITMAP_SIZE + 1, len);
            return ret;
        }
        @Override
        public boolean isTrusted() {
            return true;
        }
    };

    /** valueCreator: if there is no record in the map, create a value to return
     * Opens RandomDrawTreeMap
     */
    public RandomDrawTreeMap(
            int tableId,
            String hasherName,
            String workingDirectory,
            String name,
            Partitioner<K> partitioner,
            boolean closeEngine,
            int hashSalt,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            Fun.Function1<V, K> valueCreator,
            ExecutorService executor,
            boolean closeExecutor,
            long ramThreshold) {

        if (keySerializer == null) {
            throw new NullPointerException();
        }
        this.tableId = tableId;
        this.hasherName = hasherName;
        this.hasher = initializeHasher(hasherName);
        this.workingDirectory = workingDirectory;
        this.name = name;
        this.partitioner = partitioner;
        this.hasValues = valueSerializer != null; // null means false, we no value, only key need to be serialized
        this.closeEngine = closeEngine;
        this.closeExecutor = closeExecutor;
        this.hashSalt = hashSalt;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.valueCreator = valueCreator;
        this.defaultMapDBInitialized=new boolean[partitioner.numPartitions];

        this.executor = executor;

        this.ramThreshold = ramThreshold;
    }

    /**
     * Set the hasher
     * @param hasherName two types, lsh and default
     * @return hasher
     */
    private Hasher initializeHasher(String hasherName) {
        switch (hasherName) {
            case "lsh":
                return new LocalitySensitiveHasher(LSHServer.getLSHEngine(), tableId);
            default:
                return new DefaultHasher(hashSalt);
        }
    }

    /**
     * In the experiment, BUCKET_LENGTH=28, the total length of hash value is 32, So SEG is 2^4=16.
     * @param bucketLength
     */
    public void updateBucketLength(int bucketLength) {
        BUCKET_LENGTH = bucketLength;
        SEG = (int) Math.pow(2, 32 - BUCKET_LENGTH);
    }

    /**
     * update the d-node size and length of hash value
     * @param newNodeSize
     * @param totalHashLength
     */
    public void updateDirectoryNodeSize(int newNodeSize, int totalHashLength) {
        DIRECTORY_NODE_SIZE = newNodeSize;
        //if newNodeSize=128
        NUM_BITS_PER_COMPARISON = (int) (Math.log(DIRECTORY_NODE_SIZE) / Math.log(2));
        System.out.println("NUM_BITS_PER_COMPARISON: " + NUM_BITS_PER_COMPARISON);
        BITS_COMPARISON_MASK = 1;
        //generate the mask. 2^7 -1
        BITS_COMPARISON_MASK = (int) Math.pow(2, NUM_BITS_PER_COMPARISON) - 1;
        TOTAL_HASH_LENGTH = totalHashLength;

        MAX_TREE_LEVEL = (TOTAL_HASH_LENGTH - (32 - BUCKET_LENGTH)) / NUM_BITS_PER_COMPARISON - 1;
        System.out.println("TOTAL_HASH_LENGTH:" + TOTAL_HASH_LENGTH);
        System.out.println("MAX_TREE_LEVEL: " + MAX_TREE_LEVEL);
        BITMAP_SIZE = newNodeSize / 32;
        System.out.println("BITMAP_SIZE: " + BITMAP_SIZE);
        if (BITMAP_SIZE < 1) {
            System.out.println("Fault: the minimum allowed directory node size is 32");
            System.exit(1);
        }
    }

    //Until here, the table initialization is finished

    @Override
    public boolean containsKey(final Object o) {
        return getPeek(o) != null;
    }

    /**
     * Return given value, without updating cache statistics if {@code expireAccess()} is true
     * It also does not use {@code valueCreator} if value is not found (always returns null if not found)
     *
     * @param key key to lookup
     * @return value associated with key or null
     */
    public V getPeek(final Object key) {
        if (key == null) return null;
        final int h = hash((K) key);
        final int seg = h >>> BUCKET_LENGTH;
        final int partition = partitioner.getPartition((K) key);

        V ret;
        //search ram
        try {
            final Lock ramLock = partitionRamLock.get(partition)[seg].readLock();
            RandomDrawTreeMap.LinkedNode<K, V> ln = null;
            try {
                ramLock.lock();
                ln = getInner(key, seg, h, partition);
            } finally {
                ramLock.unlock();
            }

            if (ln == null && persistedStorages.containsKey(partition)) {
                final Lock persistLock = partitionPersistLock.get(partition)[seg].readLock();
                try {
                    persistLock.lock();
                    ln = fetchFromPersistedStorage(key, partition,
                            partitionRootRec.get(partition)[seg], h);

                } finally {
                    persistLock.unlock();
                }
            }
            ret = ln == null ? null : ln.value;
        } catch (NullPointerException npe) {
            return null;
        }
        return ret;
    }

    @Override
    public int size() {
        return (int) Math.min(sizeLong(), Integer.MAX_VALUE);
    }

    /**
     * return the number of objects in certain partition
     * @param partitionId
     * @return
     */
    private long sizeLong(int partitionId) {
        if (counterRecids != null) {
            long ret = 0;
            for (int segmentId = 0; segmentId < 16; segmentId++) {
                Lock lock = partitionRamLock.get(partitionId)[segmentId].readLock();
                try {
                    lock.lock();
                    ret += engines.get(partitionId).get(counterRecids.get(partitionId)[segmentId],
                            Serializer.LONG);
                } finally {
                    lock.unlock();
                }
            }
            return ret;
        }
        return 0;
    }

    /**
     * return the count of all partitions
     * @return
     */
    public long sizeLong() {
        //track the counters for each partition
        if (counterRecids != null) {
            long ret = 0;
            Iterator<Integer> partitionIDIterator = partitionRamLock.keySet().iterator();
            while (partitionIDIterator.hasNext()) {
                int partitionId = partitionIDIterator.next();
                ret += sizeLong(partitionId);
            }
            return ret;
        }
        //didn't track
        return 0;
    }

    public long mappingCount() {
        //method added in java 8
        return sizeLong();
    }

    private long recursiveDirCount(Engine engine, final long dirRecid) {
        Object dir = engine.get(dirRecid, DIR_SERIALIZER);
        long counter = 0;
        int dirLen = dirLen(dir);
        for (int pos = dirStart(dir); pos < dirLen; pos++) {
            long recid = dirGet(dir, pos);
            if ((recid & 1) == 0) {
                //reference to another subdir
                recid = recid >>> 1;
                counter += recursiveDirCount(engine, recid);
            } else {
                //reference to linked list, count it
                recid = recid >>> 1;
                while (recid != 0) {
                    RandomDrawTreeMap.LinkedNode n = engine.get(recid, LN_SERIALIZER);
                    if (n != null) {
                        counter++;
                        recid = n.next;
                    } else {
                        recid = 0;
                    }
                }
            }
        }
        return counter;
    }

    @Override
    public boolean isEmpty() {
        //didn't track the counters for each partition
        return sizeLong() == 0;
    }

    /**
     * find the similar vector
     * @param key the query vector id
     * @return the list of the similarity candidates
     */
    public LinkedList<K> getSimilar(
            final Object key) {
        //TODO: Finish getSimilar
        //get hash value
        final int h = hash((K) key);
        //move to left BUCKET_LENGTH, then get the seg.
        final int seg = h >>> BUCKET_LENGTH;
        final int partition = partitioner.getPartition(
                (K) (hasher instanceof LocalitySensitiveHasher ? h : key));

        LinkedList<K> lns;
        try {
            final Lock ramLock = partitionRamLock.get(partition)[seg].readLock();
            try {
                ramLock.lock();
                lns = getInnerWithSimilarity(key, seg, h, partition);
            } finally {
                ramLock.unlock();
            }

            if (lns == null || lns.size() == 0 && persistedStorages.containsKey(partition)) {
                final Lock persistLock = partitionPersistLock.get(partition)[seg].readLock();
                try {
                    persistLock.lock();
                    lns = fetchFromPersistedStorageWithSimilarity(
                            key,
                            partition,
                            partitionRootRec.get(partition)[seg],
                            h);
                } finally {
                    persistLock.unlock();
                }
            }
        } catch (NullPointerException npe) {
            //npe.printStackTrace();
            return null;
        }
        if (lns == null)
            return null;
//    return filter(lns,h);
        return lns;
    }

    /**
     * for multiFeature
     * @param key
     * @return
     */
    public LinkedList<K> getSimilar(
            final Object key,int flag) {
        //TODO: Finish getSimilar
        //get hash value
        final int h = hash((K) key,flag);
        //move to left BUCKET_LENGTH, then get the seg.
        final int seg = h >>> BUCKET_LENGTH;
        final int partition = partitioner.getPartition(
                (K) (hasher instanceof LocalitySensitiveHasher ? h : key));

        LinkedList<K> lns;
        try {
            final Lock ramLock = partitionRamLock.get(partition)[seg].readLock();
            try {
                ramLock.lock();
                lns = getInnerWithSimilarity(key, seg, h, partition);
            } finally {
                ramLock.unlock();
            }

            if (lns == null || lns.size() == 0 && persistedStorages.containsKey(partition)) {
                final Lock persistLock = partitionPersistLock.get(partition)[seg].readLock();
                try {
                    persistLock.lock();
                    lns = fetchFromPersistedStorageWithSimilarity(
                            key,
                            partition,
                            partitionRootRec.get(partition)[seg],
                            h);
                } finally {
                    persistLock.unlock();
                }
            }
        } catch (NullPointerException npe) {
            //npe.printStackTrace();
            return null;
        }
        if (lns == null)
            return null;
//    return filter(lns,h);
        return lns;
    }

    /**
     * filter the candidiates by using similaritythreshold, cost too much time
     * @param ln
     * @param QueryHashvalue
     * @return
     */
    private LinkedList<K> filter(LinkedList<K> ln,int QueryHashvalue){
        LinkedList<K> filteredLns=new LinkedList<>();
        int SimilarityThreshold=3;
        if(ln.size()!=0){
            for(K x : ln){
                final int hash_of_x=hash((K) x);
                if(Integer.bitCount(hash_of_x^QueryHashvalue) <= SimilarityThreshold){
                    filteredLns.add(x);
                }
            }
        }
        return filteredLns;
    }

    @Override
    public V get(final Object o) {
        if (o == null) return null;
        final int h = hash((K) o);
        final int seg = h >>> BUCKET_LENGTH;
        final int partition1 = partitioner.getPartition((K) o);
        int partition = 0;
        if (!(hasher instanceof LocalitySensitiveHasher)) {
            //if MainTable
            partition = Math.abs(partition1);
        } else {
            partition = partition1;
        }
        RandomDrawTreeMap.LinkedNode<K, V> ln;
        try {
            final Lock ramLock = partitionRamLock.get(partition)[seg].readLock();
            try {
                ramLock.lock();
                ln = getInner(o, seg, h, partition);
            } finally {
                ramLock.unlock();
            }

            if (ln == null && persistedStorages.containsKey(partition)) {
                final Lock persistLock = partitionPersistLock.get(partition)[seg].readLock();
                try {
                    persistLock.lock();
                    ln = fetchFromPersistedStorage(o, partition, partitionRootRec.get(partition)[seg], h);
                    if (ln == null) {
                        System.out.println("cannot find " + o + " in persisted memory ");
                    }
                } finally {
                    persistLock.unlock();
                }
            }
        } catch (Exception npe) {
            npe.printStackTrace();
            System.out.println("fetch null at partition " + partition + ", at key " + o);
            return null;
        }

        if (valueCreator == null) {
            if (ln == null)
                return null;
            return ln.value;
        }
        //value creator is set, so create and put new value
        V value = valueCreator.run((K) o);
        //there is race condition, vc could be called twice. But map will be updated only once
        V prevVal = putIfAbsent((K) o, value);

        if (prevVal != null)
            return prevVal;
        return value;
    }

    boolean testInDataSummary(StoreAppend store, Object key) {
        try {
            DataInputStream in = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(store.fileName + "-summary")));
            BloomFilter dataSummary = BloomFilter.readFrom(in, Funnels.integerFunnel());
            boolean ret = dataSummary.mightContain(key);
            in.close();
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    protected LinkedList<K> searchWithSimilarity(
            final Object key,
            Engine engine,
            long recId,
            int h) {
        LinkedList<K> ret = new LinkedList<>();
        for (int level = MAX_TREE_LEVEL; level >= 0; level--) {
            Object dir = engine.get(recId, DIR_SERIALIZER);
            if (dir == null) {
                System.out.println("cannot find dir for " + key + " with hash value " + h);
                return null;
            }
            //
            final int slot = (h >>> (level * NUM_BITS_PER_COMPARISON)) & BITS_COMPARISON_MASK;
            if (CC.ASSERT && slot > DIRECTORY_NODE_SIZE - 1) {
                throw new DBException.DataCorruption("slot too high");
            }
            recId = dirGetSlot(dir, slot);
            if (recId == 0) {
                //no such node, empty
                //search from persisted storage for the directory
                System.out.println("met a rec with 0, level: " + level + " hash: " + h);
                int[] dir1 = (int[]) dir;
                for (int i = 0; i < dir1.length; i++) {
                    System.out.println(dir1[i]);
                }
                return null;
            }
            //last bite indicates if referenced record is LinkedNode
            //if the bit is set to 1, then it is the k-node, which stores the real key value pairs
            //otherwise, it is the directory node.
            if ((recId & 1) != 0) {
                //Nan: if the node is linkedNode n, then the records start from
                //n / 2, next, next, next
                recId = recId >>> 1;
                long workingRecId = recId;
                while (true) {
                    RandomDrawTreeMap.LinkedNode<K, V> ln = engine.get(workingRecId, LN_SERIALIZER);
                    if (ln == null) {
                        return ret;
                    }
                    //don't add the same keys
                    if (ln.key != key) {
                        ret.add(ln.key);
                    }
                    if (ln.next == 0) {
                        return ret;
                    }
                    workingRecId = ln.next;
                }
            }
            recId = recId >>> 1;
        }
        return ret;
    }

    /**
     * search the exact object in index
     * @param key
     * @param engine
     * @param recId
     * @param h
     * @return
     */
    protected LinkedNode<K, V> search(Object key, Engine engine, long recId, int h) {
        for (int level = MAX_TREE_LEVEL; level >= 0; level--) {
            Object dir = engine.get(recId, DIR_SERIALIZER);
            if (dir == null) {
                return null;
            }

            final int slot = (h >>> (level * NUM_BITS_PER_COMPARISON)) & BITS_COMPARISON_MASK;
            if (CC.ASSERT && slot > DIRECTORY_NODE_SIZE) {
                throw new DBException.DataCorruption("slot too high");
            }
            recId = dirGetSlot(dir, slot);
            if (recId == 0) {
                //no such node
                //search from persisted storage for the directory
                return null;
            }
            //last bite indicates if referenced record is LinkedNode
            //if the bit is set to 1, then it is the linkednode, which stores the real key value pairs
            //otherwise, it is the directory node.
            if ((recId & 1) != 0) {
                //if the node is linkedNode n, then the records start from
                //n / 2, next, next, next
                recId = recId >>> 1;
                long workingRecId = recId;
                while (true) {
                    RandomDrawTreeMap.LinkedNode<K, V> ln = engine.get(workingRecId, LN_SERIALIZER);
                    if (ln == null) {
                        return null;
                    }
                    if (keySerializer.equals(ln.key, (K) key)) {
                        if (CC.ASSERT && hash(ln.key) != h) {
                            throw new DBException.DataCorruption("inconsistent hash");
                        }
                        return ln;
                    }
                    if (ln.next == 0) {
                        return null;
                    }
                    workingRecId = ln.next;
                }
            }
            recId = recId >>> 1;
        }
        return null;
    }

    protected LinkedList<K> fetchFromPersistedStorageWithSimilarity(
            final Object key,
            int partitionId,
            long rootRecId,
            int hashCode) {
        Iterator<PersistedStorage> persistedStorageIterator =
                persistedStorages.get(partitionId).iterator();
        HashSet<K> ret = new HashSet<K>();
        while (persistedStorageIterator.hasNext()) {
            StoreAppend persistedStorage = persistedStorageIterator.next().store;
            if (testInDataSummary(persistedStorage, hashCode)) {
                LinkedList<K> similarCandidates =
                        searchWithSimilarity(key, persistedStorage, rootRecId, hashCode);
                if (similarCandidates != null) {
                    ret.addAll(similarCandidates);
                }
            }
        }
        LinkedList<K> l = new LinkedList<K>();
        for (K k: ret) {
            l.add(k);
        }
        return l;
    }

    private LinkedNode<K, V> fetchFromPersistedStorage(
            Object key,
            int partitionId,
            long rootRecId,
            int hashCode) {
        Iterator<PersistedStorage> persistedStorageIterator =
                persistedStorages.get(partitionId).iterator();
        LinkedNode<K, V> ret = null;
        while (persistedStorageIterator.hasNext()) {
            StoreAppend persistedStorage = persistedStorageIterator.next().store;
            if (testInDataSummary(persistedStorage, key)) {
                ret = search(key, persistedStorage, rootRecId, hashCode);
                if (ret != null) {
                    break;
                }
            }
        }
        return ret;
    }

    /**
     * Get the similar objects from innner.
     * @param key
     * @param seg
     * @param h
     * @param partition
     * @return
     */
    protected LinkedList<K> getInnerWithSimilarity(
            final Object key,
            int seg,
            int h,
            int partition) {
        try {
            long recId = partitionRootRec.get(partition)[seg];
            Engine engine = engines.get(partition);
            if (((Store) engine).getCurrSize() >= ramThreshold) {
                persist(partition);
            }
            return searchWithSimilarity(key, engine, recId, h);
        } catch (NullPointerException npe) {
            return null;
        }
    }

    /**
     * get the exact same object
     * @param key
     * @param seg
     * @param h
     * @param partition
     * @return
     */
    private LinkedNode<K, V> getInner(Object key, int seg, int h, int partition) {
        try {
            long recId = partitionRootRec.get(partition)[seg];
            Engine engine = engines.get(partition);
            if (((Store) engine).getCurrSize() >= ramThreshold) {
                persist(partition);
            }
            return search(key, engine, recId, h);
        } catch (Exception npe) {
            npe.printStackTrace();
            return null;
        }
    }


    protected static int dirStart(Object dir) {
        //totally we have 128, if the dir[] is int, so we have 4, if dir[] is long, we need 2.
        return dir instanceof int[] ? 4 : 2;
    }

    protected static int dirLen(Object dir) {
        return dir instanceof int[] ?
                ((int[]) dir).length :
                ((long[]) dir).length;
    }

    protected static boolean dirIsEmpty(Object dir) {
        if (dir == null)
            return true;
        if (dir instanceof long[])
            return false;
        return ((int[]) dir).length == 4;
    }

    /**
     * take the recid from dir[4] in certain position, dir can be incremental extended.
     * @param dir
     * @param pos
     * @return the recid
     */
    protected static long dirGet(Object dir, int pos) {
        return dir instanceof int[] ?
                ((int[]) dir)[pos] :
                ((long[]) dir)[pos];
    }

    /**
     * known the dir[] and slot, get the recid
     * @param dir
     * @param slot
     * @return
     */
    protected long dirGetSlot(Object dir, int slot) {
        if (dir instanceof int[]) {
            // dir is a 128 length int array
            int[] cc = (int[]) dir;
            int pos = dirOffsetFromSlot(cc, slot);
            if (pos < 0)
                return 0;
            return cc[pos];
        } else {
            long[] cc = (long[]) dir;
            int pos = dirOffsetFromSlot(cc, slot);
            if (pos < 0)
                return 0;
            return cc[pos];
        }
    }

    /**
     * check whether the dir[] of certain slot(from 0 to 127) is empty(0->empty, 1->hasSet)
     * @param dir
     * @param slot
     * @return the slot which can indicate recid
     */
    protected int dirOffsetFromSlot(Object dir, int slot) {
        if (dir instanceof int[])
            return dirOffsetFromSlot((int[]) dir, slot);
        else
            return dirOffsetFromSlot((long[]) dir, slot);
    }

    /**
     * converts hash slot into actual offset in dir array, using bitmap
     *
     * @param dir  dir is the index in dir node, the first 4 * 32 bits is the bitmap
     *             [0][1][2][3], inside the [0], the first one is from right.
     *
     * @param slot slot is NUM_BITS_PER_COMPARISON-bits of the hash value of the key,
     *             indicating the slot in this level
     * @return negative -offset if the slot hasn't been occupied, positive offset if the slot is set
     */
    protected final int dirOffsetFromSlot(int[] dir, int slot) {
        if (CC.ASSERT && slot > DIRECTORY_NODE_SIZE - 1)
            throw new DBException.DataCorruption("slot " + slot +  " too high");
        //the bitmap is divided into BITMAP_SIZE * 32 bits, the highest few bits indicate which range
        //this slot belongs to, 00 in first, 01 in second, 10 in third, 11 in last
        int rangeDecidingBits = NUM_BITS_PER_COMPARISON - (int) (Math.log(BITMAP_SIZE) / Math.log(2));
        //rangeDecidingBits indicates where the recid should put in BITMAP, in dir[0] or dir[1] ...
        int bitmapRange = 0;
        if (BITMAP_SIZE > 1) {
            bitmapRange = slot >>> rangeDecidingBits;
        }
        //calculate the slot in range
        int slotWithinRange = slot & (int)(Math.pow(2, rangeDecidingBits) - 1);

        //check if bit at given slot is set
        //get dir[bitmapRange] is 32 bits,
        //then move the number of slotWithin Range to a certain slot in bitmap.
        //1 is set, 0 is not set
        int isSet = ((dir[bitmapRange] >>> (slotWithinRange)) & 1);
        isSet <<= 1; //multiply by two, so it is usable in multiplication

        int offset = 0;
        //dirPos -> which integer (4 bytes)
        //get how many slots have been occupied in the range prior to bitmapRange
        for (int i = 0; i < bitmapRange; i++) {
            offset += Integer.bitCount(dir[i]);
        }
        //count how many bits have been occupied (set) before slot
        //turn slot into mask for N right bits
        int maskForBitsBeforeSlots = (1 << (slotWithinRange)) - 1;
        //count how many slots have been occupied in dir[dirPos]
        //the first BITMAP_SIZE * 32 bits in the dir node are bitmap (where BITMAP_SIZE+ comes from)
        // the second item is calculating how many bits have been occupied before this slot
        // within the bitmap
        // the '+BITMAP_SIZE' is the first BITMAP_SIZE in dir array is bitmaps
        offset += BITMAP_SIZE + Integer.bitCount(dir[bitmapRange] & maskForBitsBeforeSlots);

        //turn into negative value if bit is not set, do not use conditions
        //isSet has been multiply by two, so, if the bit is set, the offset is still "offset"
        //if not set, then return a negative value indicating the recid does not exist
        return -offset + isSet * offset;
    }

    /**
     * converts hash slot into actual offset in dir array, using bitmap
     */
    protected static final int dirOffsetFromSlot(long[] dir, int slot) {
        if (CC.ASSERT && slot > 127)
            throw new DBException.DataCorruption("slot too high");

        int offset = 0;
        long v = dir[0];

        if (slot > 63) {
            offset += Long.bitCount(v);
            v = dir[1];
        }

        slot &= 63;
        long mask = ((1L) << (slot & 63)) - 1;
        offset += 2 + Long.bitCount(v & mask);

        int v2 = (int) ((v >>> (slot)) & 1);
        v2 <<= 1;

        //turn into negative value if bit is not set, do not use conditions
        return -offset + v2 * offset;
    }

    /**
     * put new record id into directory
     *
     * @param dir      the directory node reference
     * @param slot     the slot position
     * @param newRecid the new record id
     * @return updated dir node reference
     */
    protected final Object putNewRecordIdInDir(Object dir, int slot, long newRecid) {
        if (dir instanceof int[]) {
            int[] updatedDir = (int[]) dir;
            int offset = dirOffsetFromSlot(updatedDir, slot);
            //does new recid fit into integer?
            if (newRecid <= Integer.MAX_VALUE) {
                //make copy and expand it if necessary
                if (offset < 0) {
                    offset = -offset;
                    updatedDir = Arrays.copyOf(updatedDir, updatedDir.length + 1);
                    //make space for new value
                    System.arraycopy(updatedDir, offset, updatedDir, offset + 1,
                            updatedDir.length - 1 - offset);
                    //and update bitmap
                    //TODO assert slot bit was not set
                    //we assume the minimum directory node size is 32
                    int bytePos = slot / 32;// the position in first BITMAP_SIZE(0-3)
                    int bitPos = slot % 32;// the position in BITMAP(0-31)
                    updatedDir[bytePos] = (updatedDir[bytePos] | (1 << bitPos));
                } else {
                    //TODO assert slot bit was set
                    updatedDir = updatedDir.clone();
                }
                //and insert value itself
                updatedDir[offset] = (int) newRecid;
                return updatedDir;
            } else {
                //new recid does not fit into long, so upgrade to long[] and continue
                long[] dir2 = new long[updatedDir.length - 2];
                //bitmaps
                dir2[0] = ((long) updatedDir[0] << 32) | updatedDir[1] & 0xFFFFFFFFL;
                dir2[1] = ((long) updatedDir[2] << 32) | updatedDir[3] & 0xFFFFFFFFL;
                for (int i = 4; i < updatedDir.length; i++) {
                    dir2[i - 2] = updatedDir[i];
                }
                dir = dir2;
            }
        }

        //do long stuff
        long[] dir_ = (long[]) dir;
        int offset = dirOffsetFromSlot(dir_, slot);
        //make copy and expand it if necessary
        if (offset < 0) {
            offset = -offset;
            dir_ = Arrays.copyOf(dir_, dir_.length + 1);
            //make space for new value
            System.arraycopy(dir_, offset, dir_, offset + 1, dir_.length - 1 - offset);
            //and update bitmap
            //TODO assert slot bit was not set
            int bytePos = slot / 64;
            int bitPos = slot % 64;
            dir_[bytePos] = (dir_[bytePos] | (1L << bitPos));
        } else {
            //TODO assert slot bit was set
            dir_ = dir_.clone();
        }
        //and insert value itself
        dir_[offset] = newRecid;
        return dir_;
    }









}
