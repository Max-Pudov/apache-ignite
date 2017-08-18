package org.apache.ignite.examples.loader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.MemoryMetrics;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.BinaryObjectImpl;

/**
 * Created by admin on 8/17/2017.
 */
public class Loader {
    public static void main(String[] args) throws Exception {
        Ignite ignite = Ignition.start("examples/config/persistentstore/loader.xml");

        ignite.active(true);

        IgniteCache<AffinityParticleKey, BinaryObject> depohist = ignite.cache("depohist").withKeepBinary();

        try(InputStream stream = new FileInputStream("C:\\work\\log25\\zstan_2send\\zstan_2send.txt")) {
            BufferedReader in = new BufferedReader(new InputStreamReader(stream, "UTF8"));

            String str;

            Transform t = new Transform();

            int i = 1;
            int total = 100_000;

            long min = Long.MAX_VALUE;
            long sz = 0;
            long max = Long.MIN_VALUE;
            long sum = 0;

            while ((str = in.readLine()) != null) {
                Object[] obj = t.getObj(str, DepoHist.class, AffinityParticleKey.class);

                DepoHist val = (DepoHist)obj[1];

                BinaryObjectImpl binVal = ignite.binary().toBinary(val);

                AffinityParticleKey key = (AffinityParticleKey)obj[0];

                depohist.put(key, binVal);

//                BinaryObject binVal2 = depohist.get(key);
//
//                Object val2 = binVal2.deserialize();
//
//                assert val.equals(val2);

                int tmp = binVal.array().length;

                sz += tmp;

                if (tmp < min)
                    min = tmp;

                if (tmp > max)
                    max = tmp;

                sum += tmp;

                if (i % 1000 == 0)
                    ignite.log().info("Done: " + i + " of " + total);

                i++;

                break;
            }

            ignite.log().info("Stats [size=" + depohist.size() + ", sum=" + sum/1024/1024 +
                ", min=" + min + ", max=" + max + ", avg: " + sz/(float) total + ']');

            for (MemoryMetrics metrics : ignite.memoryMetrics())
                ignite.log().info(metrics.getName() + " [Fill factor: " + metrics.getPagesFillFactor()
                    + " Eviction rate: " + metrics.getEvictionRate()
                    + ']');
        }
    }
}
