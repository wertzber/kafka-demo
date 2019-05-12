package com.elad.kafka.demo.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * User: eyalge
 * Date: 21/01/13
 * Time: 17:55
 */
public class ETLEvent implements Serializable {


    private static final Logger logger = LoggerFactory.getLogger(ETLEvent.class);
    private static final String PARAMETERS_KEY = "parameters";

    private String internalId = "";//UUID.randomUUID().toString();

    private Map<String, Object> contents = new HashMap<String, Object>();



    public ETLEvent() {
    }

    public String getInternalId() {
        return internalId;
    }

    public String ensureString(String value) {
        return value != null ? ("null".equalsIgnoreCase(value) ? "" : value) : "";
    }

    public void putAll(Map<String, Object> aMap) {
        getContents().putAll(aMap);
    }

    public Map<String, Object> getContents() {
        return contents;
    }

    public Object get(String key) {
        Object value = getContents().get(key);
        if (value == null) {
            logger.debug("Null value for key [event: {},  key: {}]", getClass().getSimpleName(), key);
            /*if ("sessionId".equalsIgnoreCase(key)){
                return " ";
            }*/
            return "";
        } else if (value instanceof String) {
            String ensuredValue = ensureString((String) value);
            /*if ("sessionId".equalsIgnoreCase(key) && "".equalsIgnoreCase(ensuredValue)){
                return " ";
            }*/
            return ensuredValue;
        }
        logger.debug("Get result [event: {}, key: {}, value: {}]", getClass().getSimpleName(), key, value);
        return value;
    }

    public Object getParameter(String parametersKey, String key) {
        Object mapCandidate = get(parametersKey);
        return getParameter(mapCandidate, key);
    }

    public Object getParameter(Object mapCandidate, String key) {
        String value = "";

        if (mapCandidate != null && mapCandidate instanceof Map) {
            Map map = (Map) mapCandidate;
            if (!map.isEmpty()) {
                Object o = map.get(key);
                if (o != null) {
                    return o;
                }
            }
        }
        return value;
    }

    public String getNestedParameter(String parametersKey, String nestedKey, String key) {
        String value = "";
        Object mapCandidate = getParameter(parametersKey, nestedKey);

        if (mapCandidate != null && mapCandidate instanceof Map) {
            Map map = (Map) mapCandidate;
            if (!map.isEmpty()) {
                Object o = map.get(key);
                if (o != null) {
                    return o.toString();
                }
            }
        }
        return value;
    }

    public Object getParameter(String key) {
        return getParameter(PARAMETERS_KEY, key);
    }

    public Object put(String key, Object value) {
        return getContents().put(key, value);
    }

    public void remove(String key) {
        getContents().remove(key);
    }

    public void setContents(Map<String, Object> contents) {
        this.contents = contents;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.getClass().getSimpleName());
        stringBuilder.append("[");
        for (Map.Entry entry : contents.entrySet()) {
            stringBuilder.append(entry.getKey());
            stringBuilder.append("=");
            stringBuilder.append("'");
            stringBuilder.append(entry.getValue());
            stringBuilder.append("'");
            stringBuilder.append(",");
        }
        stringBuilder.append("]");

        return stringBuilder.toString();
    }

    public boolean checkEquals(ETLEvent other) {
        for (String key : contents.keySet()) {
            if (!ignoreThisKey(key)) {
                Object newValue = get(key);
                Object oldValue = other.get(key);
                // Check if newValue is null
                if (newValue != null) {
                    // Not null, check if oldValue if null
                    if (oldValue != null) {
                        // Not null, compare two values, if different then return false, otherwise move on to the next set of values
                        if (!newValue.toString().equalsIgnoreCase(oldValue.toString())) {
                            // Values don't match, return false
                            logger.info("{} comparison, values are different for key {}, values: {},{}", this.getClass().getSimpleName(), key, oldValue, newValue);
                            return false;
                        }
                        // Values match, move on to the next set of values
                    } else {
                        // oldValue is null but the newValue is not, so they are different
                        logger.info("{} comparison, values are different for key {}, values: {},{}", this.getClass().getSimpleName(), key, oldValue, newValue);
                        return false;
                    }
                } else if (oldValue != null) {
                    // New value is null, but the oldValue is not null, so they are different
                    logger.info("{} comparison, values are different for key {}, values: {},{}", this.getClass().getSimpleName(), key, oldValue, newValue);
                    return false;
                }
            }
        }
        // All values seem to match, return true
        return true;
    }

    protected boolean ignoreThisKey(String key) {
        //nextCommandInterval
        return key != null && (
                key.equalsIgnoreCase("eventEntryTimestamp") ||
                        key.equalsIgnoreCase("nextCommandInterval") ||
                        key.equalsIgnoreCase("id") ||
                        key.equalsIgnoreCase("uniqueId") ||
                        key.equalsIgnoreCase("locale") ||
                        key.equalsIgnoreCase("applicationIds") ||
                        key.equalsIgnoreCase("meHashed") ||
                        key.equalsIgnoreCase("lastVisit") ||
                        key.equalsIgnoreCase("displayMetrics")
        );
    }
}
