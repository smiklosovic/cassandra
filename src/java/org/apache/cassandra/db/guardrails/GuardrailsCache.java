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

package org.apache.cassandra.db.guardrails;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.JMX;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.LocalizeString;
import org.apache.cassandra.utils.MBeanWrapper;

import static java.util.Arrays.stream;
import static java.util.Collections.unmodifiableMap;
import static java.util.Comparator.comparing;

public class GuardrailsCache
{
    private static final Logger logger = LoggerFactory.getLogger(GuardrailsCache.class);

    public static final GuardrailsCache instance = new GuardrailsCache();

    private GuardrailsMBean guardrailsMBean;

    private Map<String, Method> setters;
    private Map<String, List<Method>> getters;

    private Map<String, Method> flagSetters;

    private Map<String, List<Method>> flagsGetters;
    private Map<String, List<Method>> valuesGetters;
    private Map<String, List<Method>> thresholdsGetters;

    private GuardrailsCache()
    {

    }

    public void invoke(Method method, Object valueToSet) throws InvocationTargetException, IllegalAccessException
    {
        method.invoke(guardrailsMBean, valueToSet);
    }

    public Object invoke(Method method, Object... args)
    {
        try
        {
            return method.invoke(guardrailsMBean, args);
        }
        catch (Throwable t)
        {
            return null;
        }
    }

    public Map<String, Method> getFlagSetters()
    {
        return flagSetters;
    }

    public Map<String, List<Method>> getFlagsGetters()
    {
        return flagsGetters;
    }

    public Map<String, List<Method>> getValuesGetters()
    {
        return valuesGetters;
    }

    public Map<String, List<Method>> getThresholdsGetters()
    {
        return thresholdsGetters;
    }

    public synchronized Map<String, List<Method>> getAllGetters()
    {
        return getters;
    }

    public synchronized List<Method> getAllGetters(String guardrailName)
    {
        return getters.get(guardrailName);
    }

    public synchronized Method getSetter(String guardrailName)
    {
        return setters.get(guardrailName);
    }

    public synchronized void clientInitialisation(GuardrailsMBean mBean)
    {
        initialize(mBean);
    }

    public synchronized void serverInitialisation()
    {
        try
        {
            guardrailsMBean = JMX.newMBeanProxy(MBeanWrapper.instance.getMBeanServer(),
                                                new ObjectName(Guardrails.MBEAN_NAME),
                                                GuardrailsMBean.class);

            initialize(guardrailsMBean);
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    private synchronized void initialize(GuardrailsMBean guardrailsMBean)
    {
        if (setters != null && getters != null)
            return;

        try
        {
            this.guardrailsMBean = guardrailsMBean;

            Method[] guardrailsMethods = guardrailsMBean.getClass().getDeclaredMethods();

            if (setters == null)
            {
                try
                {
                    setters = unmodifiableMap(Arrays.stream(guardrailsMethods)
                                                    .filter(method -> method.getName().startsWith("set") && !method.getName().endsWith("CSV"))
                                                    .collect(Collectors.toMap(method -> toSnakeCase(method.getName().substring(3)),
                                                                              method -> method))
                                                    .entrySet()
                                                    .stream()
                                                    .filter(p -> !ignored.contains(p.getKey()))
                                                    .sorted(Map.Entry.comparingByKey())
                                                    .collect(Collectors.toMap(Map.Entry::getKey,
                                                                              Map.Entry::getValue,
                                                                              (e1, e2) -> e1,
                                                                              LinkedHashMap::new)));

                    flagSetters = filterSetters(setters, e -> e.getValue().getParameterCount() == 1 && e.getValue().getParameterTypes()[0] == Boolean.class);
                }
                catch (Throwable t)
                {
                    // TODO not sure if this is the right thing to do
                    logger.warn("Unable to get Guardrail setters.", t);
                }
            }

            if (getters == null)
            {
                Map<String, List<Method>> allGetters = stream(guardrailsMethods)
                                                       .filter(method -> method.getName().startsWith("get")
                                                                         && !method.getName().endsWith("CSV")
                                                                         && !(method.getName().endsWith("WarnThreshold") || method.getName().endsWith("FailThreshold")))
                                                       .collect(Collectors.groupingBy(method -> toSnakeCase(method.getName().substring(3))));

                // TODO for now remove custom guardrails
                for (String ignore : ignored)
                    allGetters.remove(ignore);

                Map<String, List<Method>> thresholds = stream(guardrailsMethods)
                                                       .filter(method -> method.getName().startsWith("get")
                                                                         && !method.getName().endsWith("CSV")
                                                                         && (method.getName().endsWith("WarnThreshold") || method.getName().endsWith("FailThreshold")))
                                                       .sorted(comparing(Method::getName))
                                                       .collect(Collectors.groupingBy(method -> {
                                                           String methodName = method.getName().substring(3);
                                                           String snakeCase = toSnakeCase(methodName);
                                                           if (snakeCase.endsWith("warn_threshold"))
                                                               return snakeCase.replaceAll("_warn_", "_");
                                                           else
                                                               return snakeCase.replaceAll("_fail_", "_");
                                                       }));

                allGetters.putAll(thresholds);

                getters = unmodifiableMap(allGetters.entrySet()
                                                    .stream()
                                                    .sorted(Map.Entry.comparingByKey())
                                                    .collect(Collectors.toMap(Map.Entry::getKey,
                                                                              Map.Entry::getValue,
                                                                              (e1, e2) -> e1,
                                                                              LinkedHashMap::new)));

                flagsGetters = filterGetters(allGetters, e -> {
                    for (Method m : e.getValue())
                    {
                        if (!m.getReturnType().equals(Boolean.class) && !m.getReturnType().equals(boolean.class))
                            return false;
                    }

                    return true;
                });

                valuesGetters = filterGetters(allGetters, e -> {
                    for (Method m : e.getValue())
                    {
                        if (!m.getReturnType().equals(Set.class))
                            return false;

                        // TODO
                        // if (m.getGenericParameterTypes().length != 1 || m.getGenericParameterTypes()[0] != String.class)
                        //    return false;
                    }

                    return true;
                });

                thresholdsGetters = filterGetters(allGetters, e -> {
                    for (Method m : e.getValue())
                    {
                        if (!m.getName().endsWith("Threshold"))
                            return false;
                    }

                    return true;
                });
            }
        }
        catch (Throwable t)
        {

        }
    }

    private Map<String, Method> filterSetters(Map<String, Method> setters, Predicate<Map.Entry<String, Method>> filter)
    {
        return unmodifiableMap(setters.entrySet()
                                      .stream()
                                      .filter(filter)
                                      .sorted(Map.Entry.comparingByKey())
                                      .collect(Collectors.toMap(Map.Entry::getKey,
                                                                Map.Entry::getValue,
                                                                (e1, e2) -> e1,
                                                                LinkedHashMap::new)));
    }

    private Map<String, List<Method>> filterGetters(Map<String, List<Method>> getters, Predicate<Map.Entry<String, List<Method>>> filter)
    {
        return unmodifiableMap(getters.entrySet()
                                      .stream()
                                      .filter(filter)
                                      .sorted(Map.Entry.comparingByKey())
                                      .collect(Collectors.toMap(Map.Entry::getKey,
                                                                Map.Entry::getValue,
                                                                (e1, e2) -> e1,
                                                                LinkedHashMap::new)));
    }

    public static String toSnakeCase(String camelCase)
    {
        if (camelCase == null || camelCase.isEmpty())
            return camelCase;
        else
        {
            String maybeSnakeCase = toSnakeCaseTranslationMap.get(camelCase);
            if (maybeSnakeCase != null)
                return maybeSnakeCase;

            return LocalizeString.toLowerCaseLocalized(CAMEL_PATTERN.matcher(camelCase).replaceAll("$1_$2"));
        }
    }

    /**
     * Special map for methods which do not adhere to camel-case convention precisely.
     * These will be translated manually.
     */
    private static final Map<String, String> toSnakeCaseTranslationMap = Map.of("ZeroTTLOnTWCSEnabled", "zero_ttl_on_twcs_enabled",
                                                                                "ZeroTTLOnTWCSWarned", "zero_ttl_on_twcs_warned",
                                                                                "FieldsPerUDTFailThreshold", "fields_per_udt_fail_threshold",
                                                                                "FieldsPerUDTWarnThreshold", "fields_per_udt_warn_threshold",
                                                                                "FieldsPerUDTThreshold", "fields_per_udt_threshold",
                                                                                "SimpleStrategyEnabled", "simplestrategy_enabled",
                                                                                "NonPartitionRestrictedQueryEnabled", "non_partition_restricted_index_query_enabled");


    private static final Pattern CAMEL_PATTERN = Pattern.compile("([a-z])([A-Z])");
    private static final Set<String> ignored = Set.of("password_validator_config");
}
