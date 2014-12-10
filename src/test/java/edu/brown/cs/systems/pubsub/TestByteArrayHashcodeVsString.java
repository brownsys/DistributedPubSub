package edu.brown.cs.systems.pubsub;

import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

/**
 * Basic perf test for speed of java hashcode for bytes -> string -> hashcode vs. bytes -> hashcode(bytes)
 * @author a-jomace
 *
 */
public class TestByteArrayHashcodeVsString extends TestCase {
  
  private static final Random r = new Random();
  
  @Test
  public void testByteArrayHashcode() {
    int[] lengths_to_test = new int[] { 1, 5, 10, 50, 100};
    int num_tests = 10;
    int strings_per_test = 1000000;
    
    for (int len : lengths_to_test) {
      byte[][] stringbytes = new byte[strings_per_test][];
      for (int i = 0; i < strings_per_test; i++) {
        stringbytes[i] = RandomStringUtils.random(len).getBytes();
      }
      
      long string_duration = 0;
      long bytes_duration = 0;
      
      for (int i = 0; i < num_tests; i++) {
        if (r.nextBoolean()) {
          // String first
          long start = System.nanoTime();
          for (int j = 0; j < strings_per_test; j++) {
            new String(stringbytes[j]).hashCode();
          }
          string_duration += (System.nanoTime() - start);
          start = System.nanoTime();
          for (int j = 0; j < strings_per_test; j++) {
            ((Integer) Arrays.hashCode(stringbytes[j])).hashCode();
          }
          bytes_duration += (System.nanoTime() - start);
        } else {
          // bytes first
          long start = System.nanoTime();
          for (int j = 0; j < strings_per_test; j++) {
            ((Integer) Arrays.hashCode(stringbytes[j])).hashCode();
          }
          bytes_duration += (System.nanoTime() - start);
          start = System.nanoTime();
          for (int j = 0; j < strings_per_test; j++) {
            new String(stringbytes[j]).hashCode();
          }
          string_duration += (System.nanoTime() - start);
        }
      }
      
      System.out.println(String.format("strlen=%d  string=%.2fns bytes=%.2fns avg per word", len, string_duration/((double) strings_per_test*num_tests), bytes_duration/((double) strings_per_test*num_tests)));
    }
    
  }

  public static void main(String[] args) {
    new TestByteArrayHashcodeVsString().testByteArrayHashcode();
  }
  
}
