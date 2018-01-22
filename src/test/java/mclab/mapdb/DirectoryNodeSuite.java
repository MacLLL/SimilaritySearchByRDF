package mclab.mapdb;

import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

@SuppressWarnings({"unchecked", "rawtypes"})
public class DirectoryNodeSuite {
  static DummyPartitioner partitioner = new DummyPartitioner(1);
  static Path tempDirFile;

  @BeforeClass
  public static void onlyOnce() {
    try {
      tempDirFile = Files.createTempDirectory(String.valueOf(System.currentTimeMillis()));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test_simple_put_with_128() {
    RandomDrawTreeMap m = new RandomDrawTreeMap(
            0,
            "default",
            tempDirFile.toString(),
            "test_simple_put",
            partitioner,
            false,
            2,
            Serializer.INTEGER,
            Serializer.INTEGER,
            null,
            null,
            false,
            Long.MAX_VALUE);
    m.updateBucketLength(28);
    m.updateDirectoryNodeSize(128, 32);
    m.put(111, 222);
    m.put(333, 444);
    System.out.println(m.size());
    System.out.println(tempDirFile.toString());
    org.junit.Assert.assertTrue(m.containsKey(111));
    org.junit.Assert.assertTrue(!m.containsKey(222));
    org.junit.Assert.assertTrue(m.containsKey(333));
    org.junit.Assert.assertTrue(!m.containsKey(444));

    org.junit.Assert.assertEquals(222, m.get(111));
    org.junit.Assert.assertEquals(null, m.get(222));
    org.junit.Assert.assertEquals(444, m.get(333));
  }

  @Test
  public void test_simple_put_with_64() {
    RandomDrawTreeMap m = new RandomDrawTreeMap(
            0,
            "default",
            tempDirFile.toString(),
            "test_simple_put",
            partitioner,
            false,
            2,
            Serializer.INTEGER,
            Serializer.INTEGER,
            null,
            null,
            false,
            Long.MAX_VALUE);
    m.updateBucketLength(30);
    m.updateDirectoryNodeSize(64, 32);
    m.put(111, 222);
    m.put(333, 444);
    org.junit.Assert.assertTrue(m.containsKey(111));
    org.junit.Assert.assertTrue(!m.containsKey(222));
    org.junit.Assert.assertTrue(m.containsKey(333));
    org.junit.Assert.assertTrue(!m.containsKey(444));

    org.junit.Assert.assertEquals(222, m.get(111));
    org.junit.Assert.assertEquals(null, m.get(222));
    org.junit.Assert.assertEquals(444, m.get(333));
  }

  @Test
  public void test_simple_put_with_32() {
    RandomDrawTreeMap m = new RandomDrawTreeMap(
            0,
            "default",
            tempDirFile.toString(),
            "test_simple_put",
            partitioner,
            false,
            2,
            Serializer.INTEGER,
            Serializer.INTEGER,
            null,
            null,
            false,
            Long.MAX_VALUE);
    m.updateBucketLength(30);
    m.updateDirectoryNodeSize(32, 32);
    m.put(111, 222);
    m.put(333, 444);
    org.junit.Assert.assertTrue(m.containsKey(111));
    org.junit.Assert.assertTrue(!m.containsKey(222));
    org.junit.Assert.assertTrue(m.containsKey(333));
    org.junit.Assert.assertTrue(!m.containsKey(444));

    org.junit.Assert.assertEquals(222, m.get(111));
    org.junit.Assert.assertEquals(null, m.get(222));
    org.junit.Assert.assertEquals(444, m.get(333));
  }

}
