package de.hhu.bsinfo.dxmem.data;

import org.junit.Assert;
import org.junit.Test;

public class ChunkIDRangesTest {
    @Test
    public void inRange() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);

        for (int i = 0; i <= 10; i++) {
            Assert.assertTrue(range.isInRange(i));
        }

        for (int i = 11; i <= 20; i++) {
            Assert.assertFalse(range.isInRange(i));
        }
    }

    @Test
    public void inRange2() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);
        range.add(15, 20);

        for (int i = 0; i <= 10; i++) {
            Assert.assertTrue(range.isInRange(i));
        }

        for (int i = 11; i <= 14; i++) {
            Assert.assertFalse(range.isInRange(i));
        }

        for (int i = 15; i <= 20; i++) {
            Assert.assertTrue(range.isInRange(i));
        }
    }

    @Test
    public void inRange3() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);
        range.add(15, 20);
        range.add(100, 200);
        range.add(500, 1234);

        for (int i = 0; i <= 10; i++) {
            Assert.assertTrue(range.isInRange(i));
        }

        for (int i = 11; i <= 14; i++) {
            Assert.assertFalse(range.isInRange(i));
        }

        for (int i = 15; i <= 20; i++) {
            Assert.assertTrue(range.isInRange(i));
        }

        for (int i = 21; i <= 99; i++) {
            Assert.assertFalse(range.isInRange(i));
        }

        for (int i = 100; i <= 200; i++) {
            Assert.assertTrue(range.isInRange(i));
        }

        for (int i = 201; i <= 499; i++) {
            Assert.assertFalse(range.isInRange(i));
        }

        for (int i = 500; i <= 1234; i++) {
            Assert.assertTrue(range.isInRange(i));
        }
    }

    @Test
    public void random() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);

        for (int i = 0; i < 100000; i++) {
            long cid = range.getRandomCidWithinRanges();

            Assert.assertTrue(range.isInRange(cid));
        }
    }

    @Test
    public void random2() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);
        range.add(15, 20);
        range.add(100, 200);
        range.add(500, 1234);

        for (int i = 0; i < 100000; i++) {
            long cid = range.getRandomCidWithinRanges();

            Assert.assertTrue(range.isInRange(cid));
        }
    }

    @Test
    public void add() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);
        range.add(0);

        Assert.assertEquals(11, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add2() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);
        range.add(10);

        Assert.assertEquals(11, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add3() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);
        range.add(5);

        Assert.assertEquals(11, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add4() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);
        range.add(11);

        Assert.assertEquals(12, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add5() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);
        range.add(12);

        Assert.assertEquals(12, range.getTotalCidsOfRanges());
        Assert.assertEquals(2, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add6() {
        ChunkIDRanges range = new ChunkIDRanges(1, 10);
        range.add(0);

        Assert.assertEquals(11, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add7() {
        ChunkIDRanges range = new ChunkIDRanges(2, 10);
        range.add(0);

        Assert.assertEquals(10, range.getTotalCidsOfRanges());
        Assert.assertEquals(2, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add8() {
        ChunkIDRanges range = new ChunkIDRanges(2, 10);
        range.add(10, 12);

        Assert.assertEquals(11, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add9() {
        ChunkIDRanges range = new ChunkIDRanges(2, 10);
        range.add(12, 13);

        Assert.assertEquals(11, range.getTotalCidsOfRanges());
        Assert.assertEquals(2, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add10() {
        ChunkIDRanges range = new ChunkIDRanges(2, 10);
        range.add(5, 10);

        System.out.println(range);

        Assert.assertEquals(9, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add11() {
        ChunkIDRanges range = new ChunkIDRanges(2, 10);
        range.add(5, 20);

        Assert.assertEquals(19, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add12() {
        ChunkIDRanges range = new ChunkIDRanges(5, 20);
        range.add(5, 20);

        Assert.assertEquals(16, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add13() {
        ChunkIDRanges range = new ChunkIDRanges(5, 20);
        range.add(1, 15);

        Assert.assertEquals(20, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add14() {
        ChunkIDRanges range = new ChunkIDRanges(5, 20);
        range.add(2, 7);

        Assert.assertEquals(19, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add15() {
        ChunkIDRanges range = new ChunkIDRanges(5, 20);
        range.add(1, 4);

        Assert.assertEquals(20, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add16() {
        ChunkIDRanges range = new ChunkIDRanges(5, 20);
        range.add(1, 3);

        Assert.assertEquals(19, range.getTotalCidsOfRanges());
        Assert.assertEquals(2, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add17() {
        ChunkIDRanges range = new ChunkIDRanges(1, 5);
        range.add(7, 9);

        Assert.assertEquals(8, range.getTotalCidsOfRanges());
        Assert.assertEquals(2, range.size());
        Assert.assertFalse(range.isEmpty());

        range.add(6);

        Assert.assertEquals(9, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add18() {
        ChunkIDRanges range = new ChunkIDRanges(1, 5);
        range.add(9);

        Assert.assertEquals(6, range.getTotalCidsOfRanges());
        Assert.assertEquals(2, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add19() {
        ChunkIDRanges range = new ChunkIDRanges();
        range.add(0, 1);
        range.add(3, 5);
        range.add(1, 7);

        Assert.assertEquals(8, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add20() {
        ChunkIDRanges range = new ChunkIDRanges();
        range.add(0, 1);
        range.add(3, 5);
        range.add(7, 9);
        range.add(2, 15);

        Assert.assertEquals(16, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void add21() {
        ChunkIDRanges range = new ChunkIDRanges();
        range.add(0, 1);
        range.add(3, 5);
        range.add(7, 9);
        range.add(1, 15);

        Assert.assertEquals(16, range.getTotalCidsOfRanges());
        Assert.assertEquals(1, range.size());
        Assert.assertFalse(range.isEmpty());
    }

    @Test
    public void inRange4() {
        ChunkIDRanges range = new ChunkIDRanges(1, 10);
        Assert.assertEquals(4, range.isInRange(2, 5));
    }

    @Test
    public void inRange5() {
        ChunkIDRanges range = new ChunkIDRanges(1, 4);
        Assert.assertEquals(4, range.isInRange(1, 4));
    }

    @Test
    public void inRange6() {
        ChunkIDRanges range = new ChunkIDRanges(1, 7);
        Assert.assertEquals(5, range.isInRange(3, 8));
    }

    @Test
    public void inRange7() {
        ChunkIDRanges range = new ChunkIDRanges(5, 10);
        Assert.assertEquals(4, range.isInRange(1, 8));
    }

    @Test
    public void inRange8() {
        ChunkIDRanges range = new ChunkIDRanges(1, 5);
        range.add(7, 10);

        Assert.assertEquals(9, range.isInRange(0, 15));
    }

    @Test
    public void inRange9() {
        ChunkIDRanges range = new ChunkIDRanges(1, 5);
        range.add(7, 10);
        range.add(12, 15);

        Assert.assertEquals(4, range.isInRange(6, 10));
    }

    @Test
    public void inRange10() {
        ChunkIDRanges range = new ChunkIDRanges();

        Assert.assertEquals(0, range.isInRange(1, 10));
    }

    @Test
    public void inRange11() {
        ChunkIDRanges range = new ChunkIDRanges(1, 10);
        Assert.assertEquals(0, range.isInRange(11, 15));
    }

    @Test
    public void inRange12() {
        ChunkIDRanges range = new ChunkIDRanges(1, 10);
        Assert.assertEquals(10, range.isInRange(0, 15));
    }

    @Test
    public void remove() {
        ChunkIDRanges range = new ChunkIDRanges();
        Assert.assertFalse(range.remove(0));
    }

    @Test
    public void remove2() {
        ChunkIDRanges range = new ChunkIDRanges(0, 0);
        Assert.assertFalse(range.remove(1));
        Assert.assertEquals(1, range.size());
        Assert.assertTrue(range.remove(0));
        Assert.assertEquals(0, range.size());
    }

    @Test
    public void remove3() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);

        Assert.assertTrue(range.remove(0));
        Assert.assertEquals(1, range.size());
        Assert.assertEquals(10, range.isInRange(1, 10));
    }

    @Test
    public void remove4() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);

        Assert.assertTrue(range.remove(10));
        Assert.assertEquals(1, range.size());
        Assert.assertEquals(10, range.isInRange(0, 9));
    }

    @Test
    public void remove5() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);

        Assert.assertTrue(range.remove(5));
        Assert.assertEquals(2, range.size());
        Assert.assertFalse(range.isInRange(5));
        Assert.assertEquals(5, range.isInRange(0, 4));
        Assert.assertEquals(5, range.isInRange(6, 10));
    }

    @Test
    public void remove6() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);
        range.add(12, 15);

        Assert.assertTrue(range.remove(5));
        Assert.assertEquals(3, range.size());
        Assert.assertFalse(range.isInRange(5));
        Assert.assertEquals(5, range.isInRange(0, 4));
        Assert.assertEquals(5, range.isInRange(6, 10));
    }

    @Test
    public void iterable() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);

        long counter = 0;

        for (Long id : range) {
            Assert.assertEquals(counter, id.longValue());
            counter++;
        }
    }

    @Test
    public void iterable2() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);
        range.add(13, 20);

        long counter = 0;

        for (Long id : range) {
            Assert.assertEquals(counter, id.longValue());
            counter++;

            if (counter == 11) {
                counter = 13;
            }
        }
    }

    @Test
    public void iterable3() {
        ChunkIDRanges range = new ChunkIDRanges(0, 10);
        range.add(13, 20);
        range.add(100, 100);

        long counter = 0;

        for (Long id : range) {
            Assert.assertEquals(counter, id.longValue());
            counter++;

            if (counter == 11) {
                counter = 13;
            }

            if (counter == 21) {
                counter = 100;
            }
        }
    }
}
