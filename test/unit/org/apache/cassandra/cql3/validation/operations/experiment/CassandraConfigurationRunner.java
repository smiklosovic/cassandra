/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.validation.operations.experiment;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Paths;

import org.junit.internal.builders.AllDefaultPossibilitiesBuilder;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.RunnerBuilder;

public class CassandraConfigurationRunner extends Runner
{
    public static final String[] DEFAULT_CONFIG = new String[]{ "test/conf/cassandra.yaml" };
    public static final String[] LATEST_CONFIG = new String[]{ "test/conf/cassandra-latest.yaml" };
    public static final String[] ALL_CONFIGS = new String[]{ "test/conf/cassandra.yaml", "test/conf/cassandra-latest.yaml" };

    private static final Class<? extends Runner> DEFAULT_RUNNER_CLASS = BlockJUnit4ClassRunner.class;

    private final String[] cassandraYamls;
    private final Runner runner;

    public CassandraConfigurationRunner(Class<?> testClass) throws Throwable
    {
        Class<? extends Runner> runnerClass = DEFAULT_RUNNER_CLASS;

        if (testClass.isAnnotationPresent(CassandraConfiguration.class))
        {
            CassandraConfiguration configuration = testClass.getAnnotation(CassandraConfiguration.class);

            if (configuration.allConfigs())
                cassandraYamls = ALL_CONFIGS;
            else if (configuration.latestConfig())
                cassandraYamls = LATEST_CONFIG;
            else
                cassandraYamls = DEFAULT_CONFIG;

            runnerClass = configuration.runner();
        }
        else
        {
            cassandraYamls = DEFAULT_CONFIG;
        }

        runner = buildRunner(runnerClass, testClass);
    }

    @Override
    public Description getDescription()
    {
        return runner.getDescription();
    }

    @Override
    public void run(RunNotifier notifier)
    {
        for (String yaml : cassandraYamls)
        {
            System.setProperty("cassandra.config", "file://" + Paths.get(yaml).toAbsolutePath());
            runner.run(notifier);
        }
    }

    private static Runner buildRunner(Class<? extends Runner> runnerClass, Class<?> testClass) throws Throwable
    {
        try
        {
            return runnerClass.getConstructor(Class.class).newInstance(testClass);
        }
        catch (NoSuchMethodException e)
        {
            return runnerClass.getConstructor(Class.class, RunnerBuilder.class)
                              .newInstance(testClass, new AllDefaultPossibilitiesBuilder(true));
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface CassandraConfiguration
    {
        boolean latestConfig() default false;
        boolean allConfigs() default false;

        Class<? extends Runner> runner() default BlockJUnit4ClassRunner.class;
    }
}
