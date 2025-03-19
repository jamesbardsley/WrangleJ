package com.jamesbardsley.wranglej;

import com.jamesbardsley.wranglej.annotations.WrangleField;
import com.jamesbardsley.wranglej.annotations.WranglePrimary;
import com.jamesbardsley.wranglej.annotations.WrangleSecondary;
import com.jamesbardsley.wranglej.annotations.WrangleSecondarySource;
import com.jamesbardsley.wranglej.annotations.enums.WrangleJoinType;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * WrangleJ is a utility class designed to map raw data (provided as Maps) into annotated
 * POJOs (Plain Old Java Objects). It uses annotations to define data sources, join operations,
 * and how fields in the POJO are populated from the input data. This class is intended to
 * facilitate easy and automatic transformation of data outputs from APIs or databases.
 *
 * <p>Usage:
 * Define a class and annotate it with {@code @WranglePrimary} to specify the main data source.
 * Use {@code @WrangleSecondary} to define additional data sources to join with the primary source.
 * Annotate fields in the class with {@code @WrangleField} to map raw data paths to POJO fields.</p>
 */
public class WrangleJ {
    /**
     * Transforms raw data into a Stream of POJOs using specified class annotations.
     *
     * @param <T>     The type of the class to be transformed.
     * @param clazz   The target class, annotated with {@code @WranglePrimary} and optionally
     *                {@code @WrangleSecondary}.
     * @param primary The primary data source, provided as an Iterable of maps.
     * @param sources A map of secondary source names to their respective data sources.
     * @return A Stream of objects of type {@code T}, populated with data mapped from the input sources.
     * @throws WrangleException If the class lacks a default constructor or required annotations,
     *                          or if a required secondary source is missing.
     */
    public static <T> Stream<T> wrangle(Class<T> clazz, Iterable<Map<String, Object>> primary,
                                        Map<String, Iterable<Map<String, Object>>> sources) {

        return wrangle(clazz, StreamSupport.stream(primary.spliterator(), false), sources);
    }

    /**
     * Retrieves the default constructor of the specified class.
     *
     * @param clazz The target class.
     * @param <T>   The type of the class.
     * @return The default constructor of the class.
     * @throws WrangleException If the class does not have a default constructor.
     */
    private static <T> Constructor<T> getDefaultConstructor(Class<T> clazz) {
        Constructor<T> defaultConstructor;

        try {
            defaultConstructor = clazz.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new WrangleException("Attempted to wrangle a class with no default constructor: " + clazz.getName());
        }

        return defaultConstructor;
    }

    /**
     * Ensures that the given class is annotated with {@code @WranglePrimary}.
     *
     * @param clazz The class to check.
     * @param <T>   The type of the class.
     * @throws WrangleException If the {@code @WranglePrimary} annotation is missing.
     */
    private static <T> void checkWranglePrimary(Class<T> clazz) {
        if (!clazz.isAnnotationPresent(WranglePrimary.class)) {
            throw new WrangleException(
                    String.format(
                            "Attempted to wrangle a class which is not annotated with @WranglePrimary: %s",
                            clazz.getName()));
        }
    }

    /**
     * Validates that all required secondary sources are provided.
     *
     * @param clazz            The target class, annotated with {@code @WrangleSecondary}.
     * @param secondarySources A map of the provided secondary source names to their data.
     * @param <T>              The type of the class.
     * @throws WrangleException If a required secondary source is missing.
     */
    private static <T> void checkSecondarySourcesPresent(Class<T> clazz,
                                                         Map<String, Iterable<Map<String, Object>>> secondarySources) {

        if (clazz.isAnnotationPresent(WrangleSecondary.class)) {
            WrangleSecondary wrangleSecondary = clazz.getAnnotation(WrangleSecondary.class);
            WrangleSecondarySource[] secondarySourcesRequired = wrangleSecondary.secondarySources();

            for (WrangleSecondarySource source : secondarySourcesRequired) {
                if (!secondarySources.containsKey(source.name())) {
                    throw new WrangleException(String.format("%s requires a secondary source named %s but it has not been provided.",
                            clazz.getName(), source.name()));
                }
            }
        }
    }

    /**
     * Transforms raw data (in the form of a Stream) into a Stream of POJOs using provided class annotations.
     * <p>
     * This method performs:
     * - Validation of the class annotations {@code @WranglePrimary} and {@code @WrangleSecondary}.
     * - Joining of primary and secondary data sources based on specified join fields.
     * - Field mapping to populate the target POJO fields with data from joined rows.
     *
     * @param <T>           The type of the class to be transformed.
     * @param clazz         The target class, annotated with {@code @WranglePrimary} and
     *                      optionally {@code @WrangleSecondary}.
     * @param primaryStream The primary data source, provided as a Stream of maps.
     * @param sources       A map of secondary source names to their respective data sources.
     * @return A Stream of objects of type {@code T}, populated with data mapped from the input sources.
     * @throws WrangleException If the class lacks a default constructor or required annotations,
     *                          or if a required secondary source is missing.
     */
    public static <T> Stream<T> wrangle(Class<T> clazz, Stream<Map<String, Object>> primaryStream,
                                        Map<String, Iterable<Map<String, Object>>> sources) {

        checkWranglePrimary(clazz);
        checkSecondarySourcesPresent(clazz, sources);
        Constructor<T> defaultConstructor = getDefaultConstructor(clazz);

        Map<String, Map<String, Collection<Map<String, Object>>>> indexedSources = createSourceIndexes(clazz, sources);

        return primaryStream
                .flatMap(primaryRow -> joinToPrimaryRow(clazz, primaryRow, indexedSources).stream())
                .map(joinedRow -> {
                    T instance = createInstance(defaultConstructor);

                    for (Field field : clazz.getDeclaredFields()) {
                        if (field.isAnnotationPresent(WrangleField.class)) {
                            WrangleField wrangleField = field.getAnnotation(WrangleField.class);

                            // TODO: Concatenation and arithmetic
                            Object val = getValueBySearchPath(wrangleField.direct(), joinedRow).orElse(null);
                            try {
                                field.setAccessible(true);
                                field.set(instance, val);
                            } catch (IllegalAccessException e) {
                                throw new WrangleException("Exception setting value " + val + " on field " + field.getName() + ": " + e.getMessage());
                            }
                        }
                    }

                    return instance;
                });
    }

    /**
     * Creates indexed maps of secondary sources for efficient joining with primary rows.
     *
     * @param clazz   The target class, annotated with {@code @WrangleSecondary}.
     * @param sources A map of secondary source names to their respective data sources.
     * @return A nested map of source names to join key values, mapping to a collection of rows
     * matching each key value.
     */
    private static Map<String, Map<String, Collection<Map<String, Object>>>> createSourceIndexes(Class clazz,
                                                                                                 Map<String, Iterable<Map<String, Object>>> sources) {
        WrangleSecondary wrangleSecondary = (WrangleSecondary)clazz.getAnnotation(WrangleSecondary.class);

        // TODO: All these maps should really be keyed by Comparable instead of String
        Map<String, Map<String, Collection<Map<String, Object>>>> indexedSources = new HashMap<>();

        for (WrangleSecondarySource source : wrangleSecondary.secondarySources()) {
            String sourceName = source.name();
            String rightJoinField = source.joinRight();
            String sourceRemovedKey = rightJoinField.substring(rightJoinField.indexOf('.') + 1);

            indexedSources.put(sourceName, new HashMap<>());

            for (Map<String, Object> row : sources.get(sourceName)) {
                Optional<Object> rightJoinVal = getValueBySearchPath(sourceRemovedKey, row);

                if (rightJoinVal.isPresent()) {
                    indexedSources.get(sourceName).putIfAbsent(rightJoinVal.get().toString(), new ArrayList<>());
                    indexedSources.get(sourceName).get(rightJoinVal.get().toString()).add(row);
                }
            }
        }

        return indexedSources;
    }

    /**
     * Joins a primary row with matching rows from secondary sources based on join fields.
     *
     * @param clazz          The target class, annotated with {@code @WrangleSecondary}.
     * @param primaryRow     The primary row to be joined.
     * @param indexedSources A map of secondary sources indexed by join key values.
     * @return A list of maps representing joined rows, where each map contains data
     * from the primary source and any matching secondary sources.
     */
    private static List<Map<String, Map<String, Object>>> joinToPrimaryRow(Class clazz, Map<String, Object> primaryRow,
                                                                           Map<String, Map<String, Collection<Map<String, Object>>>> indexedSources) {

        WranglePrimary primary = (WranglePrimary)clazz.getAnnotation(WranglePrimary.class);
        WrangleSecondary wrangleSecondary = (WrangleSecondary)clazz.getAnnotation(WrangleSecondary.class);
        WrangleSecondarySource[] secondarySources = wrangleSecondary.secondarySources();

        String primaryName = primary.name();

        if (secondarySources.length == 0) {
            // If there are no sources to join to then just wrap the primary row up with a map of its name
            return List.of(Map.of(primaryName, primaryRow));
        }

        List<Map<String, Map<String, Object>>> result = new ArrayList<>();
        result.add(Map.of(primaryName, primaryRow));

        // Progressively build up the result by adding the matching objects from each subsequent source.
        for (WrangleSecondarySource source : secondarySources) {
            String sourceName = source.name();

            List<Map<String, Map<String, Object>>> newLeftSide = new ArrayList<>();

            for (Map<String, Map<String, Object>> leftSideRow : result) {
                // Get the value of the left side join key
                Optional<Object> optLeft = getValueBySearchPath(source.joinLeft(), leftSideRow);

                if (optLeft.isEmpty()) continue;

                // Get the matching rows from the right side from our indexed map.
                Collection<Map<String, Object>> matchingRightRows = indexedSources.get(sourceName).get(optLeft.get().toString());

                if (matchingRightRows == null) {
                    if (source.joinType() == WrangleJoinType.OUTER) {
                        // If we're outer joining then keep the rows from our left side in the results.
                        newLeftSide.add(leftSideRow);
                    }
                } else {
                    for (Map<String, Object> rightRow : matchingRightRows) {
                        Map<String, Map<String, Object>> newLeftSideRow = new HashMap<>();

                        newLeftSideRow.putAll(leftSideRow);
                        newLeftSideRow.put(sourceName, rightRow);

                        newLeftSide.add(newLeftSideRow);
                    }
                }
            }

            result = newLeftSide;
        }

        return result;
    }

    /**
     * Retrieves a value from a nested map structure based on a dot-delimited search path.
     *
     * @param path      The dot-delimited path to the desired value (e.g., "student.firstName").
     * @param searchObj The map to search within.
     * @return An {@code Optional} containing the retrieved value, or empty if the path does not exist.
     */
    private static Optional<Object> getValueBySearchPath(String path, Map<String, ? extends Object> searchObj) {
        String[] pathParts = path.split("\\.");
        Object curObj = searchObj;

        for (String part : pathParts) {
            if (!(curObj instanceof Map)) {
                return Optional.empty();
            }

            curObj = ((Map)curObj).get(part);
        }

        return Optional.ofNullable(curObj);
    }

    /**
     * Creates an instance of a class using its default constructor.
     *
     * @param defaultConstructor The default constructor of the class.
     * @param <T>                The type of the class.
     * @return A new instance of the class.
     * @throws WrangleException If instantiation fails due to access issues, exceptions,
     *                          or other reasons.
     */
    private static <T> T createInstance(Constructor<T> defaultConstructor) {
        try {
            return defaultConstructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new WrangleException("Failed during instantiation: " + e.getMessage());
        }
    }
}