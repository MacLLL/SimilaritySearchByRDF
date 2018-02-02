package mclab.mapdb;


import breeze.stats.distributions.Rand;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import mclab.deploy.LSHServer;
import mclab.deploy.SingleFeatureRDFInit;
import mclab.lsh.DefaultHasher;
import mclab.lsh.Hasher;
import mclab.lsh.LocalitySensitiveHasher;
import mclab.lsh.vector.SparseVector;
import mclab.utils.Serializers;
import scala.Array;
import scala.collection.mutable.StringBuilder;
import mclab.deploy.HashTableInit;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

@SuppressWarnings({"unchecked", "rawtypes"})
public class RandomDrawTreeMap<K, V>
        extends AbstractMap<K, V>
        implements ConcurrentMap<K, V>,
        Closeable {

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
    protected boolean defaultMapDBInitialized[] = null;

    /**
     * keep the number of objects in each sub-index
     */
    private int[] numberOfObjectsInEachPartition = null;

    /**
     * Indicates if this collection collection was not made by DB by user.
     * If user can not access DB object, we must shutdown Executor and close Engine ourself
     * in close() method.
     */
    protected final boolean closeExecutor;

    public final ExecutorService executor;

    private final int structureLockScale = 256;

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
     * persistedStorage
     */
    protected ConcurrentHashMap<Integer, PriorityQueue<PersistedStorage>> persistedStorages =
            new ConcurrentHashMap<Integer, PriorityQueue<PersistedStorage>>();

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
     *
     * @return no mean
     */
    public void getAllRootRecIdForEachSegment() {
        for (int i = 0; i < partitionRootRec.size(); i++) {
            System.out.println("partition #" + i + "has" + partitionRootRec.get(i));
            for (long x : partitionRootRec.get(i)) {
                System.out.print(x + ",");
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
     *
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

    /**
     * valueCreator: if there is no record in the map, create a value to return
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
        this.defaultMapDBInitialized = new boolean[partitioner.numPartitions];

        this.executor = executor;

        this.ramThreshold = ramThreshold;

        this.numberOfObjectsInEachPartition = new int[partitioner.numPartitions];
    }

    /**
     * Set the hasher
     *
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
     *
     * @param bucketLength
     */
    public void updateBucketLength(int bucketLength) {
        BUCKET_LENGTH = bucketLength;
        SEG = (int) Math.pow(2, 32 - BUCKET_LENGTH);
    }

    /**
     * update the d-node size and length of hash value
     *
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
    public int size() {
        return (int) Math.min(sizeLong(), Integer.MAX_VALUE);
    }

    /**
     * return the number of objects in certain partition
     *
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
     *
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
     * Todo the 1-step wise partition search adaptive
     * find the similar vector by the key in dataTable
     *
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
     * calculate the all sub-index within steps
     *
     * @param currentSubIndexID
     * @param steps
     * @return sub-index list within steps
     */
    private ArrayList<Integer> findStepWiseSubIndexIDs(final int currentSubIndexID, final int steps) {
        ArrayList<Integer> subIndexIDList = new ArrayList<>();
        for (int subIndexID = 0; subIndexID < partitioner.numPartitions; subIndexID++) {
            if (Integer.bitCount(subIndexID ^ currentSubIndexID) <= steps)
                subIndexIDList.add(subIndexID);
        }
        return subIndexIDList;

    }

    /**
     * Todo the 1-step wise partition search adaptive
     * find the similar vector by the key in dataTable
     *
     * @param key the query vector id
     * @return the list of the similarity candidates
     */
    public LinkedList<K> getSimilarWithStepWise(
            final Object key, final int steps) {
        //TODO: Finish getSimilar with steps partition search
        //get hash value
        final int h = hash((K) key);
        //move to left BUCKET_LENGTH, then get the seg.
        final int seg = h >>> BUCKET_LENGTH;
        final int partition = partitioner.getPartition(
                (K) (hasher instanceof LocalitySensitiveHasher ? h : key));
        final ArrayList<Integer> allSubIndexs = findStepWiseSubIndexIDs(partition, steps);
        LinkedList<K> finalLns = new LinkedList<>();
        for (int i = 0; i < allSubIndexs.size(); i++) {
            int currentPartition = allSubIndexs.get(i);
            LinkedList<K> tmpLns;
            try {
                final Lock ramLock = partitionRamLock.get(currentPartition)[seg].readLock();
                try {
                    ramLock.lock();
                    tmpLns = getInnerWithSimilarity(key, seg, h, currentPartition);
                } finally {
                    ramLock.unlock();
                }
                if (tmpLns == null || tmpLns.size() == 0 && persistedStorages.containsKey(currentPartition)) {
                    final Lock persistLock = partitionPersistLock.get(currentPartition)[seg].readLock();
                    try {
                        persistLock.lock();
                        tmpLns = fetchFromPersistedStorageWithSimilarity(
                                key,
                                currentPartition,
                                partitionRootRec.get(currentPartition)[seg],
                                h);
                    } finally {
                        persistLock.unlock();
                    }
                }
            } catch (NullPointerException npe) {
                //npe.printStackTrace();
                tmpLns = null;
            }
            if (tmpLns != null)
                finalLns.addAll(tmpLns);
        }
        if (finalLns == null)
            return null;
        return finalLns;
    }


    /**
     * for multiFeature
     *
     * @param key
     * @return
     */
    public LinkedList<K> getSimilar(
            final Object key, int flag) {
        //TODO: Finish getSimilar
        //get hash value
        final int h = hash((K) key, flag);
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
     *
     * @param ln
     * @param QueryHashvalue
     * @return
     */
    private LinkedList<K> filter(LinkedList<K> ln, int QueryHashvalue) {
        LinkedList<K> filteredLns = new LinkedList<>();
        int SimilarityThreshold = 3;
        if (ln.size() != 0) {
            for (K x : ln) {
                final int hash_of_x = hash((K) x);
                if (Integer.bitCount(hash_of_x ^ QueryHashvalue) <= SimilarityThreshold) {
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
//                System.out.println("met a rec with 0, level: " + level + " hash: " + h);
//                int[] dir1 = (int[]) dir;
//                for (int i = 0; i < dir1.length; i++) {
//                    System.out.println(dir1[i]);
//                }
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
     *
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
        for (K k : ret) {
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
     *
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
                runPersistTask(partition);
            }
            return searchWithSimilarity(key, engine, recId, h);
        } catch (NullPointerException npe) {
            return null;
        }
    }

    /**
     * get the exact same object
     *
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
                runPersistTask(partition);
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
     *
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
     *
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
     *
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
     * @param slot slot is NUM_BITS_PER_COMPARISON-bits of the hash value of the key,
     *             indicating the slot in this level
     * @return negative -offset if the slot hasn't been occupied, positive offset if the slot is set
     */
    protected final int dirOffsetFromSlot(int[] dir, int slot) {
        if (CC.ASSERT && slot > DIRECTORY_NODE_SIZE - 1)
            throw new DBException.DataCorruption("slot " + slot + " too high");
        //the bitmap is divided into BITMAP_SIZE * 32 bits, the highest few bits indicate which range
        //this slot belongs to, 00 in first, 01 in second, 10 in third, 11 in last
        int rangeDecidingBits = NUM_BITS_PER_COMPARISON - (int) (Math.log(BITMAP_SIZE) / Math.log(2));
        //rangeDecidingBits indicates where the recid should put in BITMAP, in dir[0] or dir[1] ...
        int bitmapRange = 0;
        if (BITMAP_SIZE > 1) {
            bitmapRange = slot >>> rangeDecidingBits;
        }
        //calculate the slot in range
        int slotWithinRange = slot & (int) (Math.pow(2, rangeDecidingBits) - 1);

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
                    //set to 1,means isSet
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

    /**
     * remove the slot in dir
     *
     * @param dir
     * @param slot
     * @return
     */
    protected final Object dirRemove(Object dir, final int slot) {
        int offset = dirOffsetFromSlot(dir, slot);
        if (CC.ASSERT && offset <= 0) {
            throw new DBException.DataCorruption("offset too low");
        }

        if (dir instanceof int[]) {
            int[] dir_ = (int[]) dir;
            //shrink and copy data
            int[] dir2 = new int[dir_.length - 1];
            System.arraycopy(dir_, 0, dir2, 0, offset);
            System.arraycopy(dir_, offset + 1, dir2, offset, dir2.length - offset);

            //unset bitmap bit
            //TODO assert slot bit was set
            int bytePos = slot / 32;
            int bitPos = slot % 32;
            dir2[bytePos] = (dir2[bytePos] & ~(1 << bitPos));
            return dir2;
        } else {
            long[] dir_ = (long[]) dir;
            //shrink and copy data
            long[] dir2 = new long[dir_.length - 1];
            System.arraycopy(dir_, 0, dir2, 0, offset);
            System.arraycopy(dir_, offset + 1, dir2, offset, dir2.length - offset);

            //unset bitmap bit
            //TODO assert slot bit was set
            int bytePos = slot / 64;
            int bitPos = slot % 64;
            dir2[bytePos] = (dir2[bytePos] & ~(1L << bitPos));
            return dir2;
        }
    }

    /**
     * initialize the Store layer, for off-heap storage
     *
     * @param partitionId
     * @param lockScale
     * @return
     */
    private StoreSegment initPartitionInner(int partitionId, int lockScale) {
        StoreSegment storeSegment = new StoreSegment(
                "partition-" + partitionId, Volume.UNSAFE_VOL_FACTORY, null, lockScale, 0, false, false,
                null, false, true, null);
        storeSegment.serializer = LN_SERIALIZER;//Linked-List serializer
        storeSegment.init();
        return storeSegment;
    }

    /**
     * init each partition's engine,rootrec and counter of each rec, every time ,it needs to use 8 long
     *
     * @param partitionId
     */
    private void initPartition(int partitionId) {
        // to simulate the default mapdb setup
        // all partition shares the same store with the lockscale of 16,
        // since we only have 16 segments which is default setup in concurrentHashMap
        StoreSegment store = initPartitionInner(partitionId, 16);
        Long[] segIds = new Long[SEG];
        for (int i = 0; i < SEG; i++) {
            long partitionRoot = store.put(new int[BITMAP_SIZE], DIR_SERIALIZER);
//      System.out.println("SegIds["+i+"]="+partitionRoot);
            //partitionRootRec.put(partitionId, partitionRoot);
            segIds[i] = partitionRoot;
        }
        //initialize counterRecId
        Long[] counterRecIdArray = new Long[SEG];
        for (int i = 0; i < SEG; i++) {
            long counterRecId = store.put(0L, Serializer.LONG);
//      System.out.println("initial counterRecId:" + counterRecId);
            counterRecIdArray[i] = counterRecId;
        }
        //each partition is the same, it's the big bug!!
//    for (int pId = 0; pId < partitioner.numPartitions; pId++) {
//      engines.put(pId, store);
//      partitionRootRec.put(pId, segIds);
//      counterRecids.put(pId, counterRecIdArray);
//    }
        engines.put(partitionId, store);
        partitionRootRec.put(partitionId, segIds);
        counterRecids.put(partitionId, counterRecIdArray);
    }

    protected String buildStorageName(int partitionId, int segId) {
        StringBuilder sb = new StringBuilder();
        sb.append("partition-" + partitionId + "-" + segId);
        return sb.toString();
    }

    protected void initPartitionIfNecessary(int partitionId) {
        // simulate default mapdb
        try {
            initStorageLock.writeLock().lock();
            if (!defaultMapDBInitialized[partitionId]) {
                initPartition(partitionId);
                ReentrantReadWriteLock[] ramLockArray = new ReentrantReadWriteLock[SEG];
                ReentrantReadWriteLock[] persistLockArray = new ReentrantReadWriteLock[SEG];
                for (int i = 0; i < SEG; i++) {
                    ramLockArray[i] = new ReentrantReadWriteLock();
                    persistLockArray[i] = new ReentrantReadWriteLock();
                }
                //only when each partition is going to be used, we initialize it.
                partitionRamLock.put(partitionId, ramLockArray);
                partitionPersistLock.put(partitionId, persistLockArray);
                defaultMapDBInitialized[partitionId] = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            initStorageLock.writeLock().unlock();
        }
    }

    /***
     * calculate the hash value, which is depended on the HashName
     *
     * @param key the key of object in dataTable
     * @return
     */

    public int hash(final K key) {
        if (hasher instanceof LocalitySensitiveHasher) {
            // the hasher is the locality sensitive hasher, where we need to calculate the hash of the
            // vector instead of the key value
            SparseVector v = SingleFeatureRDFInit.vectorIdToVector().get(key);
            if (v == null) {
                System.out.println("fetch vector " + key + ", but got NULL");
                System.exit(1);
            }
            return hasher.hash(v, Serializers.VectorSerializer());
        } else {
            // the hasher is the default hasher which calculates the hash based on the key directly
            return hasher.hash(key, keySerializer);
        }
    }


    /***
     * For multi feature when test dataset CC_WEB_VIDEO
     *
     * @param key
     * @param flag
     * @return
     */

    public int hash(final K key, int flag) {
        if (hasher instanceof LocalitySensitiveHasher) {
            // the hasher is the locality sensitive hasher, where we need to calculate the hash of the
            // vector instead of the key value
            SparseVector v = null;
            if (flag == 1) {
                v = HashTableInit.vectorIdToVector_blue().get(key);
            } else if (flag == 2) {
                v = HashTableInit.vectorIdToVector_green().get(key);
            } else if (flag == 3) {
                v = HashTableInit.vectorIdToVector_red().get(key);
            }
            if (v == null) {
                System.out.println("fetch vector " + key + ", but got NULL");
                System.exit(1);
            }
            return hasher.hash(v, Serializers.VectorSerializer());
//      }
        } else {
            // the hasher is the default hasher which calculates the hash based on the key directly
            return hasher.hash(key, keySerializer);
        }
    }


    /**
     * get each size of sub-index
     */
    public void getPartitionNums() {
        for (int i = 0; i < numberOfObjectsInEachPartition.length; i++) {
            System.out.println("Partition " + i + " has " + numberOfObjectsInEachPartition[i] + " objects.");
        }
    }

    @Override
    public V put(final K key, final V value) {

        if (key == null)
            throw new IllegalArgumentException("null key");

        if (value == null)
            throw new IllegalArgumentException("null value");

        V ret;
        final int h = hash(key);
        final int seg = h >>> BUCKET_LENGTH;
        final int partition = partitioner.getPartition(
                (K) (hasher instanceof LocalitySensitiveHasher ? h : key));
        initPartitionIfNecessary(partition);
        if (hasher instanceof LocalitySensitiveHasher)
            numberOfObjectsInEachPartition[partition]++;
        try {
            partitionRamLock.get(partition)[seg].writeLock().lock();
            ret = putInner(key, value, h, partition);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            partitionRamLock.get(partition)[seg].writeLock().unlock();
        }
        return value;
    }

    /***
     * for multi feature
     *
     * @param key
     * @param value
     * @param flag
     * @return
     */
    public V put(final K key, final V value, int flag) {
        if (key == null)
            throw new IllegalArgumentException("null key");

        if (value == null)
            throw new IllegalArgumentException("null value");

        V ret;
        final int h = hash(key, flag);
        final int seg = h >>> BUCKET_LENGTH;
        final int partition = partitioner.getPartition(
                (K) (hasher instanceof LocalitySensitiveHasher ? h : key));
        initPartitionIfNecessary(partition);
//    if(hasher instanceof LocalitySensitiveHasher){
//      times[partition]++;
////      System.out.println("times =" + times[partition] +" Put the key:" + key+" and value:"+value+" in partition:" + partition);
//    }
        try {
            partitionRamLock.get(partition)[seg].writeLock().lock();
            ret = putInner(key, value, h, partition);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            partitionRamLock.get(partition)[seg].writeLock().unlock();
        }

        return value;
    }

    /**
     * update the kv pair in the segment
     *
     * @param key       key
     * @param value     value
     * @param h         hashcode of kv pair
     * @param partition the target sub-index
     * @return null if the corresponding kv pair doesn't exist, otherwise return the existing value
     */
    protected V putInner(K key, V value, int h, int partition) {
        int seg = h >>> BUCKET_LENGTH;
        long dirRecid = partitionRootRec.get(partition)[seg];
        Engine engine = engines.get(partition);

        int level = MAX_TREE_LEVEL;
        while (true) {
            Object dir = engine.get(dirRecid, DIR_SERIALIZER);
            //every NUM_BITS_PER_COMPARISON bits present the slot ID of the record
            final int slot = (h >>> (NUM_BITS_PER_COMPARISON * level)) & BITS_COMPARISON_MASK;

            if (CC.ASSERT && (slot > DIRECTORY_NODE_SIZE - 1))
                throw new DBException.DataCorruption("slot too high");
            if (dir == null) {
                //create new dir
                dir = new int[BITMAP_SIZE]; //16 bytes, 128 bits
            }
            //dirOffset - the offset with in a dir
            final int dirOffset = dirOffsetFromSlot(dir, slot);
            int bucketConflictCost = 0; // the threshold number of objects in this level
            long recid = dirOffset < 0 ? 0 : dirGet(dir, dirOffset);
            if (recid != 0) {
                //the record id has existed
                if ((recid & 1) == 0) {
                    //this is the d-node
                    dirRecid = recid >>> 1;
                    level--;
                    continue;
                }
                recid = recid >>> 1;

                //traverse linked list, try to replace previous value
                LinkedNode<K, V> ln = engine.get(recid, LN_SERIALIZER);

                while (ln != null) {
                    if (keySerializer.equals(ln.key, key)) {
                        //if found, replace value at this node
                        V oldVal = ln.value;
                        ln = new LinkedNode<K, V>(ln.next, ln.key, value);
                        if (CC.ASSERT && ln.next == recid)
                            throw new DBException.DataCorruption("cyclic reference in linked list");
                        engine.update(recid, ln, LN_SERIALIZER);
                        return oldVal;
                    }
                    recid = ln.next; // for traverse linked list
                    ln = ((recid == 0) ? null : engine.get(recid, LN_SERIALIZER));
                    if (CC.ASSERT && ln != null && ln.next == recid)
                        throw new DBException.DataCorruption("cyclic reference in linked list");
                    bucketConflictCost++;
                    if (CC.ASSERT && bucketConflictCost > 1024 * 1024)
                        throw new DBException.DataCorruption("linked list too large");
                }
                //key was not found at linked list, so just append it to beginning
            }

            //there is no such a null value
            //check if linked list has overflow and needs to be expanded to new dir level
            if (bucketConflictCost >= BUCKET_OVERFLOW && level >= 1) {
                Object newDirNode = new int[BITMAP_SIZE];
                {
                    //Generate the new linkedNode
                    final LinkedNode<K, V> node = new LinkedNode<K, V>(0, key, value);
                    //put the linkedNode to node and get the assigned record id
                    final long newRecid = engine.put(node, LN_SERIALIZER);
                    if (CC.ASSERT && newRecid == node.next)
                        throw new DBException.DataCorruption("cyclic reference in linked list");
                    //add newly inserted record
                    //find the position of the node in the directory node in next level
                    final int pos = (h >>> (NUM_BITS_PER_COMPARISON * (level - 1))) & BITS_COMPARISON_MASK;
                    //update the dir node with the new LinkedNode,
                    // make the last bit as 0 to indicate the d-node
//          newDirNode = putNewRecordIdInDir(newDirNode, pos, (newRecid << 1) | 1);
                    newDirNode = putNewRecordIdInDir(newDirNode, pos, newRecid << 1);
                }

                //redistribute linked bucket into new dir
                //traverse all linked node under the same slot and put it in the new directory node
                //in the next level
                long nodeRecid = dirOffset < 0 ? 0 : dirGet(dir, dirOffset) >>> 1;
                while (nodeRecid != 0) {
                    //get the node
                    LinkedNode<K, V> n = engine.get(nodeRecid, LN_SERIALIZER);
                    //take the next at first, since it will change latter
                    final long nextRecid = n.next;
                    //calculate the position in next level
                    final int pos = (hash(n.key) >>> (NUM_BITS_PER_COMPARISON * (level - 1))) &
                            BITS_COMPARISON_MASK;
                    //get the recid in newDirNode by slot
                    final long recid2 = dirGetSlot(newDirNode, pos);
                    n = new LinkedNode<K, V>(recid2 >>> 1, n.key, n.value);
                    //put in the new record node, since it's a k-node, set the last bit as 1
                    newDirNode = putNewRecordIdInDir(newDirNode, pos, (nodeRecid << 1) | 1);
                    engine.update(nodeRecid, n, LN_SERIALIZER);
                    if (CC.ASSERT && nodeRecid == n.next)
                        throw new DBException.DataCorruption("cyclic reference in linked list");
                    nodeRecid = nextRecid;
                }

                //insert nextDir and update parent dir
                long nextDirRecid = engine.put(newDirNode, DIR_SERIALIZER);
                int parentPos = (h >>> (NUM_BITS_PER_COMPARISON * level)) & BITS_COMPARISON_MASK;
                //update the parent directory node
                dir = putNewRecordIdInDir(dir, parentPos, nextDirRecid << 1);
                engine.update(dirRecid, dir, DIR_SERIALIZER);
                //update counter
                counter(partition, seg, engine, +1);
                return null;
            } else {
                // record does not exist in linked list and the linked list hasn't overflow,
                // so create new one
                recid = dirOffset < 0 ? 0 : dirGet(dir, dirOffset) >>> 1;
                //insert at the head of the linked list
                //the recid/2 === the first record under this slot
                //because the last bit label the type of node, and here we know it's a k-node
                final long newRecid = engine.put(
                        new LinkedNode<K, V>(recid, key, value),
                        LN_SERIALIZER);
                if (CC.ASSERT && newRecid == recid) {
                    throw new DBException.DataCorruption("cyclic reference in linked list");
                }

                dir = putNewRecordIdInDir(dir, slot, (newRecid << 1) | 1);
                engine.update(dirRecid, dir, DIR_SERIALIZER);
                //update counter
                counter(partition, seg, engine, +1);
                return null;
            }
        }
    }

    /**
     * count the number in each seg of sub-index
     *
     * @param partition
     * @param seg
     * @param engine
     * @param plus
     */
    protected void counter(int partition, int seg, Engine engine, int plus) {
        if (counterRecids == null) {
            return;
        }

        long oldCounter = engine.get(counterRecids.get(partition)[seg], Serializer.LONG);
        oldCounter += plus;
        engine.update(counterRecids.get(partition)[seg], oldCounter, Serializer.LONG);
    }

    /**
     * remove the object in index
     *
     * @param key
     * @return
     */
    @Override
    public V remove(Object key) {
        V ret;

        final int h = hash((K) key);
        final int seg = h >>> BUCKET_LENGTH;
        final int partition = partitioner.getPartition((K) key);
        try {
            partitionRamLock.get(partition)[seg].writeLock().lock();
            ret = removeInternal(key, partition, h);
        } finally {
            partitionRamLock.get(partition)[seg].writeLock().unlock();
        }
        return ret;
    }

    protected V removeInternal(Object key, int partition, int h) {
        Engine engine = engines.get(partition);
        int seg = h >>> BUCKET_LENGTH;
        final long[] dirRecids = new long[4];
        int level = 3;
        dirRecids[level] = partitionRootRec.get(partition)[seg];

        while (true) {
            Object dir = engine.get(dirRecids[level], DIR_SERIALIZER);
            final int slot = (h >>> (7 * level)) & 0x7F;
            if (CC.ASSERT && slot > 127)
                throw new DBException.DataCorruption("slot too high");

            if (dir == null) {
                //create new dir
                dir = new int[4];
            }

            long recid = dirGetSlot(dir, slot);

            if (recid != 0) {
                if ((recid & 1) == 0) {
                    level--;
                    dirRecids[level] = recid >>> 1;
                    continue;
                }
                recid = recid >>> 1;

                //traverse linked list, try to remove node
                LinkedNode<K, V> ln = engine.get(recid, LN_SERIALIZER);
                LinkedNode<K, V> prevLn = null;
                long prevRecid = 0;
                while (ln != null) {
                    if (keySerializer.equals(ln.key, (K) key)) {
                        //remove from linkedList
                        if (prevLn == null) {
                            //referenced directly from dir
                            if (ln.next == 0) {
                                recursiveDirDelete(engine, h, level, dirRecids, dir, slot);


                            } else {
                                dir = putNewRecordIdInDir(dir, slot, (ln.next << 1) | 1);
                                engine.update(dirRecids[level], dir, DIR_SERIALIZER);
                            }

                        } else {
                            //referenced from LinkedNode
                            prevLn = new LinkedNode<K, V>(ln.next, prevLn.key, prevLn.value);
                            engine.update(prevRecid, prevLn, LN_SERIALIZER);
                            if (CC.ASSERT && prevRecid == prevLn.next)
                                throw new DBException.DataCorruption("cyclic reference in linked list");
                        }
                        //found, remove this node
                        if (CC.ASSERT && !(hash(ln.key) == h))
                            throw new DBException.DataCorruption("inconsistent hash");
                        engine.delete(recid, LN_SERIALIZER);
                        counter(partition, seg, engine, -1);
                        return ln.value;
                    }
                    prevRecid = recid;
                    prevLn = ln;
                    recid = ln.next;
                    ln = recid == 0 ? null : engine.get(recid, LN_SERIALIZER);
//                        counter++;
                }
                //key was not found at linked list, so it does not exist
                return null;
            }
            //recid is 0, so entry does not exist
            return null;

        }
    }

    private void recursiveDirDelete(Engine engine, int h, int level, long[] dirRecids, Object dir,
                                    int slot) {
        //was only item in linked list, so try to collapse the dir
        dir = dirRemove(dir, slot);

        if (dirIsEmpty(dir)) {
            //delete from parent dir
            if (level == 3) {
                //parent is segment, recid of this dir can not be modified,  so just update to null
                engine.update(dirRecids[level], new int[4], DIR_SERIALIZER);
            } else {
                engine.delete(dirRecids[level], DIR_SERIALIZER);

                final Object parentDir = engine.get(dirRecids[level + 1], DIR_SERIALIZER);
                final int parentPos = (h >>> (7 * (level + 1))) & 0x7F;
                recursiveDirDelete(engine, h, level + 1, dirRecids, parentDir, parentPos);
                //parentDir[parentPos>>>DIV8][parentPos&MOD8] = 0;
                //engine.update(dirRecids[level + 1],parentDir,DIR_SERIALIZER);

            }
        } else {
            engine.update(dirRecids[level], dir, DIR_SERIALIZER);
        }
    }

    @Override
    public void clear() {
        Iterator<Integer> partitionIds = partitionRamLock.keySet().iterator();
        while (partitionIds.hasNext()) {
            int partitionId = partitionIds.next();
            for (int segId = 0; segId < SEG; segId++) {
                partitionRamLock.get(partitionId)[segId].writeLock().lock();
                try {
                    Engine engine = engines.get(partitionId);

                    if (counterRecids != null) {
                        engine.update(counterRecids.get(partitionId)[segId], 0L, Serializer.LONG);
                    }

                    Long[] dirRecs = partitionRootRec.get(partitionId);
                    for (int i = 0; i < dirRecs.length; i++) {
                        final long dirRecid = dirRecs[i];
                        recursiveDirClear(engine, dirRecid);
                        //set dir to null, as segment recid is immutable
                        engine.update(dirRecid, new int[BITMAP_SIZE], DIR_SERIALIZER);
                    }

                } finally {
                    partitionRamLock.get(partitionId)[segId].writeLock().unlock();
                }
            }
        }
    }

    private void recursiveDirClear(Engine engine, final long dirRecid) {
        final Object dir = engine.get(dirRecid, DIR_SERIALIZER);
        if (dir == null)
            return;
        int dirlen = dirLen(dir);
        for (int offset = dirStart(dir); offset < dirlen; offset++) {
            long recid = dirGet(dir, offset);
            if ((recid & 1) == 0) {
                //another dir
                recid = recid >>> 1;
                //recursively remove dir
                recursiveDirClear(engine, recid);
                engine.delete(recid, DIR_SERIALIZER);
            } else {
                //linked list to delete
                recid = recid >>> 1;
                while (recid != 0) {
                    LinkedNode n = engine.get(recid, LN_SERIALIZER);
                    if (CC.ASSERT && n.next == recid)
                        throw new DBException.DataCorruption("cyclic reference in linked list");
                    engine.delete(recid, LN_SERIALIZER);
                    recid = n.next;
                }
            }
        }
    }

    @Override
    public boolean containsValue(Object value) {
        for (V v : values()) {
            if (valueSerializer.equals(v, (V) value)) return true;
        }
        return false;
    }

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


    /**
     * implement the class EntrySet of map
     */
    protected class EntrySet extends AbstractSet<Entry<K, V>> {

        private int partitionId = 0;

        public EntrySet(int partition) {
            this.partitionId = partition;
        }

        @Override
        public int size() {
            return RandomDrawTreeMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return RandomDrawTreeMap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            if (o instanceof Entry) {
                Entry e = (Entry) o;
                Object val = RandomDrawTreeMap.this.get(e.getKey());
                return val != null && valueSerializer.equals((V) val, (V) e.getValue());
            } else
                return false;
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new EntryIterator(partitionId);
        }

        @Override
        public boolean add(Entry<K, V> kvEntry) {
            K key = kvEntry.getKey();
            V value = kvEntry.getValue();
            if (key == null || value == null) throw new NullPointerException();
            RandomDrawTreeMap.this.put(key, value);
            return true;
        }

        @Override
        public boolean remove(Object o) {
            if (o instanceof Entry) {
                Entry e = (Entry) o;
                Object key = e.getKey();
                if (key == null) return false;
                return RandomDrawTreeMap.this.remove(key, e.getValue());
            }
            return false;
        }


        @Override
        public void clear() {
            RandomDrawTreeMap.this.clear();
        }
    }

    /**
     * implement the class ValueSet of map
     */
    protected class ValueSet extends AbstractCollection<V> {

        private int partitionId = 0;

        public ValueSet(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public int size() {
            return RandomDrawTreeMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return RandomDrawTreeMap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return RandomDrawTreeMap.this.containsValue(o);
        }

        @Override
        public Iterator<V> iterator() {
            return new ValueIterator(partitionId);
        }
    }

    protected class KeySet extends AbstractSet<K> implements Closeable {

        private int partitionId = 0;

        public KeySet(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public int size() {
            return RandomDrawTreeMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return RandomDrawTreeMap.this.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return RandomDrawTreeMap.this.containsKey(o);
        }

        @Override
        public Iterator<K> iterator() {
            return new KeyIterator(partitionId);
        }

        @Override
        public boolean add(K k) {
            if (RandomDrawTreeMap.this.hasValues) {
                throw new UnsupportedOperationException();
            } else {
                return RandomDrawTreeMap.this.put(k, (V) Boolean.TRUE) == null;
            }
        }

        @Override
        public boolean remove(Object o) {
            return RandomDrawTreeMap.this.remove(o) != null;
        }


        @Override
        public void clear() {
            RandomDrawTreeMap.this.clear();
        }

        public RandomDrawTreeMap<K, V> parent() {
            return RandomDrawTreeMap.this;
        }

        @Override
        public int hashCode() {
            int result = 0;
            for (K k : this) {
                result += keySerializer.hashCode(k);
            }
            return result;

        }

        @Override
        public void close() {
            RandomDrawTreeMap.this.close();
        }

        public RandomDrawTreeMap getRandomDrawTreeMap() {
            return RandomDrawTreeMap.this;
        }
    }

    private HashMap<Integer, KeySet> _keySets = new HashMap<Integer, KeySet>();

    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException("you have to indicate partitionId for getting keySet");
    }

    public Set<K> keySet(int partitionId) {
        if (!_keySets.containsKey(partitionId)) {
            _keySets.put(partitionId, new KeySet(partitionId));
        }
        return _keySets.get(partitionId);
    }

    public Collection<V> values() {
        throw new UnsupportedOperationException("you have to specify the partition ID");
    }

    private final HashMap<Integer, ValueSet> _values = new HashMap<Integer, ValueSet>();

    public ValueSet values(int partitionId) {
        if (!_values.containsKey(partitionId)) {
            _values.put(partitionId, new ValueSet(partitionId));
        }
        return _values.get(partitionId);
    }

    private final HashMap<Integer, EntrySet> _entrySet = new HashMap<>();

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException("you have to specify the partition ID");
    }

    public EntrySet entrySet(int partitionId) {
        if (!_entrySet.containsKey(partitionId)) {
            _entrySet.put(partitionId, new EntrySet(partitionId));
        }
        return _entrySet.get(partitionId);
    }


    abstract class HashIterator {

        protected LinkedNode[] currentLinkedList;
        protected int currentLinkedListPos = 0;

        private K lastReturnedKey = null;

        protected int partition = 0;

        private int lastSegment = 0;

        HashIterator(int partition) {
            this.partition = partition;
            currentLinkedList = findNextLinkedNode(0);
        }

        public void remove() {
            final K keyToRemove = lastReturnedKey;
            if (lastReturnedKey == null)
                throw new IllegalStateException();

            lastReturnedKey = null;
            RandomDrawTreeMap.this.remove(keyToRemove);
        }

        public boolean hasNext() {
            return currentLinkedList != null && currentLinkedListPos < currentLinkedList.length;
        }

        protected void moveToNext() {
            lastReturnedKey = (K) currentLinkedList[currentLinkedListPos].key;

            currentLinkedListPos += 1;
            if (currentLinkedListPos == currentLinkedList.length) {
                final int lastHash = hash(lastReturnedKey);
                currentLinkedList = advance(lastHash);
                currentLinkedListPos = 0;
            }
        }

        private LinkedNode[] advance(int lastHash) {
            int segment = lastHash >>> BUCKET_LENGTH;
            int partitionId = partition;
            Engine engine = engines.get(partitionId);
            //two phases, first find old item and increase hash
            Lock lock = partitionRamLock.get(partitionId)[segment].readLock();
            lock.lock();
            long recId;
            try {
                long dirRecid = partitionRootRec.get(partitionId)[segment];
                int level = MAX_TREE_LEVEL;
                //dive into tree, finding last hash position
                while (true) {
                    Object dir = engine.get(dirRecid, DIR_SERIALIZER);
                    //check if we need to expand deeper
                    recId = dirGetSlot(dir, (lastHash >>> (NUM_BITS_PER_COMPARISON * level)) & BITS_COMPARISON_MASK);
                    if (recId == 0 || (recId & 1) == 1) {
                        //increase hash by 1
                        if (level != 0) {
                            //down to the next level and plus 1
                            lastHash = ((lastHash >>> (NUM_BITS_PER_COMPARISON * level)) + 1) << (NUM_BITS_PER_COMPARISON * level); //should use mask and XOR
                        } else {
                            //last level, just increase by 1
                            lastHash += 1;
                        }
                        if (lastHash == 0) {
                            return null;
                        }
                        break;
                    }
                    //reference is dir, move to next level
                    dirRecid = recId >> 1;
                    level--;
                }
            } finally {
                lock.unlock();
            }
            return findNextLinkedNode(lastHash);
        }

        private LinkedNode[] findNextLinkedNode(int hash) {
            //second phase, start search from increased hash to find next items
            for (int segment = Math.max(hash >>> BUCKET_LENGTH, lastSegment); segment < SEG; segment++) {
                Engine engine = engines.get(partition);
                if (partitionRamLock.containsKey(partition)) {
                    final Lock lock = partitionRamLock.get(partition)[segment].readLock();
                    try {
                        lock.lock();
                        lastSegment = Math.max(segment, lastSegment);
                        long dirRecid = partitionRootRec.get(partition)[segment];
                        LinkedNode ret[] = findNextLinkedNodeRecur(engine, dirRecid, hash, MAX_TREE_LEVEL);
                        if (ret != null) {
                            return ret;
                        }
                        hash = 0;
                    } finally {
                        lock.unlock();
                    }
                }
            }
            return null;
        }

        private LinkedNode[] findNextLinkedNodeRecur(
                Engine engine,
                long dirRecid,
                int newHash,
                int level) {
            final Object dir = engine.get(dirRecid, DIR_SERIALIZER);
            if (dir == null)
                return null;
            int offset = Math.abs(dirOffsetFromSlot(dir, (newHash >>> (level * NUM_BITS_PER_COMPARISON)) & BITS_COMPARISON_MASK));

            boolean first = true;
            int dirlen = dirLen(dir);
            while (offset < dirlen) {
                long recid = offset < 0 ? 0 : dirGet(dir, offset);
                if (recid != 0) {
                    if ((recid & 1) == 1) {
                        recid = recid >> 1;
                        //found linked list, load it into array and return
                        LinkedNode[] array = new LinkedNode[1];
                        int arrayPos = 0;
                        while (recid != 0) {
                            LinkedNode ln = engine.get(recid, LN_SERIALIZER);
                            if (ln == null) {
                                break;
                            }
                            //increase array size if needed
                            if (arrayPos == array.length) {
                                array = Arrays.copyOf(array, array.length + 1);
                            }
                            array[arrayPos++] = ln;
                            recid = ln.next;
                        }
                        return array;
                    } else {
                        //found another dir, continue dive
                        recid = recid >> 1;
                        LinkedNode[] ret = findNextLinkedNodeRecur(engine, recid, first ? newHash : 0,
                                level - 1);
                        if (ret != null) return ret;
                    }
                }
                first = false;
                offset += 1;
            }
            return null;
        }
    }

    class KeyIterator extends HashIterator implements Iterator<K> {

        public KeyIterator(int partitionId) {
            super(partitionId);
        }

        @Override
        public K next() {
            if (currentLinkedList == null) {
                throw new NoSuchElementException();
            }
            K key = (K) currentLinkedList[currentLinkedListPos].key;
            moveToNext();
            return key;
        }
    }

    class ValueIterator extends HashIterator implements Iterator<V> {

        public ValueIterator(int partitionId) {
            super(partitionId);
        }

        @Override
        public V next() {
            if (currentLinkedList == null)
                throw new NoSuchElementException();
            V value = (V) currentLinkedList[currentLinkedListPos].value;
            moveToNext();
            return value;
        }
    }

    class EntryIterator extends HashIterator implements Iterator<Entry<K, V>> {

        public EntryIterator(int partitionId) {
            super(partitionId);
        }

        @Override
        public Entry<K, V> next() {
            if (currentLinkedList == null) {
                throw new NoSuchElementException();
            }
            K key = (K) currentLinkedList[currentLinkedListPos].key;
            moveToNext();
            return new Entry2(key);
        }
    }

    class Entry2 implements Entry<K, V> {

        private final K key;

        Entry2(K key) {
            this.key = key;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return RandomDrawTreeMap.this.get(key);
        }

        @Override
        public V setValue(V value) {
            return RandomDrawTreeMap.this.put(key, value);
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof Entry) && keySerializer.equals(key, (K) ((Entry) o).getKey());
        }

        @Override
        public int hashCode() {
            final V value = RandomDrawTreeMap.this.get(key);
            return (key == null ? 0 : keySerializer.hashCode(key)) ^
                    (value == null ? 0 : value.hashCode());
        }
    }

    /**
     * put the kv pair if it does not exist in the map
     *
     * @param key
     * @param value
     * @return
     */
    @Override
    public V putIfAbsent(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();

        final int h = hash(key);
        final int seg = h >>> BUCKET_LENGTH;
        final int partition = partitioner.getPartition(key);

        V ret;

        try {
            partitionRamLock.get(partition)[seg].writeLock().lock();
            LinkedNode<K, V> ln = RandomDrawTreeMap.this.getInner(key, seg, h, partition);
            if (ln == null)
                ret = put(key, value);
            else
                ret = ln.value;

        } finally {
            partitionRamLock.get(partition)[seg].writeLock().unlock();
        }
        return ret;
    }

    @Override
    public boolean remove(Object key, Object value) {
        if (key == null || value == null)
            throw new NullPointerException();

        boolean ret;

        final int h = hash((K) key);
        final int seg = h >>> BUCKET_LENGTH;
        final int partition = partitioner.getPartition((K) key);

        try {
            partitionRamLock.get(partition)[seg].writeLock().lock();
            LinkedNode otherVal = getInner(key, seg, h, partition);
            ret = (otherVal != null && valueSerializer.equals((V) otherVal.value, (V) value));
            if (ret) {
                removeInternal(key, partition, h);
            }
        } finally {
            partitionRamLock.get(partition)[seg].writeLock().unlock();
        }

        return ret;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        if (key == null || oldValue == null || newValue == null)
            throw new NullPointerException();

        boolean ret;

        final int h = hash(key);
        final int seg = h >>> BUCKET_LENGTH;
        final int partition = partitioner.getPartition(key);

        partitionRamLock.get(partition)[seg].writeLock().lock();
        try {
            LinkedNode<K, V> ln = getInner(key, seg, h, partition);
            ret = (ln != null && valueSerializer.equals(ln.value, oldValue));
            if (ret)
                putInner(key, newValue, h, partition);

        } finally {
            partitionRamLock.get(partition)[seg].writeLock().unlock();
        }
        return ret;
    }

    @Override
    public V replace(K key, V value) {
        if (key == null || value == null)
            throw new NullPointerException();
        V ret;
        final int h = hash(key);
        final int seg = h >>> BUCKET_LENGTH;
        final int partition = partitioner.getPartition(key);

        try {
            partitionRamLock.get(partition)[seg].writeLock().lock();
            if (getInner(key, seg, h, partition) != null)
                ret = putInner(key, value, h, partition);
            else
                ret = null;
        } finally {
            partitionRamLock.get(partition)[seg].writeLock().unlock();
        }
        return ret;
    }

    //until here, we finish all the methods in map operations.


    /**
     * get all the engines
     *
     * @return
     */
    public Collection<Engine> getEngine() {
        return engines.values();
    }


    class PersistedStorage implements Comparable<PersistedStorage> {
        long timeStamp;

        StoreAppend store;

        public PersistedStorage(long timeStamp, StoreAppend persistedStore) {
            this.timeStamp = timeStamp;
            store = persistedStore;
        }

        @Override
        public int compareTo(PersistedStorage o) {
            return timeStamp > o.timeStamp ? 1 : -1;
        }
    }


    /**
     * <p>
     * Make readonly snapshot view of current Map. Snapshot is immutable and not affected by
     * modifications made by other threads.
     * Useful if you need consistent view on Map.
     * </p><p>
     * Maintaining snapshot have some overhead, underlying Engine is closed after Map view is GCed.
     * Please make sure to release reference to this Map view, so snapshot view can be garbage
     * collected.
     * </p>
     *
     * @return snapshot
     */
    public Map<K, V> snapshot() {
        HashMap<Integer, Engine> snapshots = new HashMap<Integer, Engine>();
        //TODO thread unsafe if underlying engines are not thread safe
        Iterator<Integer> keyIterator = engines.keySet().iterator();
        while (keyIterator.hasNext()) {
            int partition = keyIterator.next();
            snapshots.put(partition, TxEngine.createSnapshotFor(engines.get(partition)));
        }
        return new RandomDrawTreeMap<K, V>(
                tableId,
                hasherName,
                workingDirectory,
                name,
                partitioner,
                closeEngine,
                hashSalt,
                keySerializer,
                valueSerializer,
                null,
                executor,
                false,
                ramThreshold);
    }


    /**
     * close all engines
     */
    @Override
    public void close() {
        //shutdown all associated objects
        if (executor != null && closeExecutor && !executor.isTerminated()) {
            executor.shutdown();
            try {
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new DBException.Interrupted(e);
            }
        }

        if (closeEngine) {
            Iterator<Integer> keyIterator = engines.keySet().iterator();
            while (keyIterator.hasNext()) {
                int key = keyIterator.next();
                engines.get(key).close();
            }
        }
    }

    private void releaseAllLocksOfPartition(int partitionId) {
        for (int i = 0; i < SEG; i++) {
            partitionRamLock.get(partitionId)[i].writeLock().unlock();
            partitionPersistLock.get(partitionId)[i].writeLock().unlock();
        }
    }

    /**
     * lock the parition
     *
     * @param partitionId
     * @return
     */
    private boolean tryLockPartition(int partitionId) {
        for (int i = 0; i < SEG; i++) {
            if (!partitionPersistLock.get(partitionId)[i].writeLock().tryLock()) {
                return false;
            }
            if (!partitionRamLock.get(partitionId)[i].writeLock().tryLock()) {
                return false;
            }
        }
        return true;
    }

    /**
     * save the index into disk(SSD)
     *
     * @param partitionId
     */
    public void runPersistTask(final int partitionId) {
        //TODO: when integrate with Spark, we shall use Spark's threadpool
        if (!partitionPersistLock.containsKey(partitionId)) {
            return;
        }
        if (executor != null) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    //TODO: we can use snapshot to allow concurrent write threads
                    long persistTimestamp = System.currentTimeMillis();
                    if (!tryLockPartition(partitionId)) {
                        //persist is ongoing
                        return;
                    }
                    try {
                        StoreSegment engine = (StoreSegment) engines.get(partitionId);
                        //engine.compact();
                        String unionDir = workingDirectory + "/" + name + "/" + partitionId;
                        File dir = new File(unionDir);
                        dir.mkdirs();
                        long startTime = System.nanoTime();
                        Store persistStorage = engine.persist(unionDir + "/" + persistTimestamp);
                        addPersistedStorage(partitionId, persistTimestamp, (StoreAppend) persistStorage);
                        long dataSummaryStartTime = System.nanoTime();
                        generateDataSummary(partitionId);
                        long dataSummaryEndTime = System.nanoTime();
                        long endTime = System.nanoTime();
                        long totalDuration = endTime - startTime;
                        long dataSummaryDuration = dataSummaryEndTime - dataSummaryStartTime;
                        System.out.println(totalDuration + " " + dataSummaryDuration);
                        initPartition(partitionId);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        releaseAllLocksOfPartition(partitionId);
                    }
                }
            });
        } else {
            LOG.warning("executor is null, rejecting to persist");
        }
    }

    private void addPersistedStorage(int partitionId, long timestamp, StoreAppend persistedStore) {
        if (!persistedStorages.containsKey(partitionId)) {
            persistedStorages.put(partitionId, new PriorityQueue<PersistedStorage>());
        }
        persistedStorages.get(partitionId).add(new PersistedStorage(timestamp, persistedStore));
    }

    private void generateDataSummary(int partitionId) {
        StoreAppend persistStorage = persistedStorages.get(partitionId).peek().store;
        persistStorage.initDataSummary((int) sizeLong(partitionId), 0.001);
        KeyIterator keyIterator = new KeyIterator(partitionId);
        while (keyIterator.hasNext()) {
            K key = keyIterator.next();
            persistStorage.updateDataSummary((Integer) key);
        }
        persistStorage.persistDataSummary();
    }

    public void initStructureLocks() {
//        System.out.println("run the RandomDrawTreeMap initStructureLocks");
        //initialize structureLocks for initializing partition structure
        for (int i = 0; i < structureLockScale; i++) {
            structureLocks.put(i, new ReentrantReadWriteLock());
        }
        //whether do this is depends
        for (int i = 0; i < partitioner.numPartitions; i++) {
            initPartitionIfNecessary(i);
        }
    }


    /**
     * return a list of objects percentage for each subIndexID
     *
     * @return
     */
    public List<Double> allSubIndexObjectsNumberDistribution() {
        long totalNum = sizeLong();
        List<Double> numList = new ArrayList<>(partitioner.numPartitions);
        for (int subIndexID = 0; subIndexID < partitioner.numPartitions; subIndexID++) {
            double tmp = sizeLong(subIndexID) / (totalNum * 1.0);
            numList.add(tmp);
        }
        return numList;

    }
}
