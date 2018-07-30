/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Base externalizable test class.
 */
public class IgniteWebConsoleConfigurationTest extends GridCommonAbstractTest {
    private static Map<Class<?>, Set<String>> PROPERTIES = new HashMap<>();

    @Override protected void beforeTestsStarted() throws Exception {
        Set<String> igniteCfgProps = new HashSet<>();

        igniteCfgProps.add("localHost");
        igniteCfgProps.add("atomicConfiguration");
        igniteCfgProps.add("userAttributes");
        igniteCfgProps.add("binaryConfiguration");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");
        igniteCfgProps.add("");

//        atomicConfiguration

        PROPERTIES.put(IgniteConfiguration.class, igniteCfgProps);

        Set<String> atomicCfgProps = new HashSet<>();

        atomicCfgProps.add("cacheMode");
        atomicCfgProps.add("atomicSequenceReserveSize");
        atomicCfgProps.add("backups");

        PROPERTIES.put(AtomicConfiguration.class, atomicCfgProps);

        Set<String> binaryCfgProps = new HashSet<>();

        binaryCfgProps.add("idMapper");
        binaryCfgProps.add("nameMapper");
        binaryCfgProps.add("serializer");
        binaryCfgProps.add("typeConfigurations");
        binaryCfgProps.add("compactFooter");

        PROPERTIES.put(BinaryConfiguration.class, binaryCfgProps);

        Set<String> binaryTypeCfgProps = new HashSet<>();

        binaryTypeCfgProps.add("idMapper");
        binaryTypeCfgProps.add("nameMapper");
        binaryTypeCfgProps.add("serializer");
        binaryTypeCfgProps.add("enum");

        PROPERTIES.put(BinaryTypeConfiguration.class, binaryTypeCfgProps);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerializationTopicCreatedByGridUuidAndUUID() throws Exception {
        Map<Class<?>, Set<String>> result = new HashMap<>();

        for (Map.Entry<Class<?>, Set<String>> forCls : PROPERTIES.entrySet()) {
            Class<?> cls = forCls.getKey();
            Set<String> props = forCls.getValue();

            Method[] methods = cls.getMethods();

            Map<String, Integer> clsProps = new HashMap<>();

            for (Method m : methods) {
                String mtdName = m.getName();

                String propName = null;

                if (mtdName.startsWith("get") || mtdName.startsWith("set"))
                    propName = new String(new char[] {mtdName.charAt(3)}).toLowerCase() + mtdName.substring(4);
                else if (mtdName.startsWith("is"))
                    propName = new String(new char[] {mtdName.charAt(2)}).toLowerCase() + mtdName.substring(3);

                if (propName != null) {
                    if (clsProps.containsKey(propName))
                        clsProps.put(propName, clsProps.get(propName) + 1);
                    else
                        clsProps.put(propName, 1);
                }
            }

            Set<String> missedField = new HashSet<>();
            Set<String> exclude = new HashSet<>();

            for (Map.Entry<String, Integer> ent : clsProps.entrySet())
                if (ent.getValue() < 2)
                    exclude.add(ent.getKey());

            for (String key: exclude)
                clsProps.remove(key);

            for (Map.Entry<String, Integer> ent : clsProps.entrySet())
                System.out.println(ent.getKey() + ": " + ent.getValue());
        }
    }
}