package com.aerospike.movement.util.core.math;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Distribution {

    /**
     * [ value: count,
     *   value: count,
     *   ...
     * ]
     * <a href="https://web.archive.org/web/20171102222704/https://www.datastax.com/2017/03/graphoendodonticology">...</a>
     * gremlin> g.V().groupCount().by(both().count())
     * ==>
     * [
     * 16:1,
     * 2:55,
     * 3:16,
     * 4:6,
     * 5:5,
     * 6:4,
     * 7:3,
     * 8:2,
     * 9:5,
     * 10:1,
     * 27:1,
     * 28:1
     * ]
     **/

    /**
     * create a number line
     * iterate the values passed in the samples,
     * add the number of units to the line given by the values occurrences
     * note the start and stop location on the number line for that value
     * now you have a stretch of the number line proportional to the number of occurrences of that value.
     * generate a random number R from 0 to numberLine.size()
     * the range R falls within indicates what value to choose.
     *
     * likelihoodToIncreaseCount is the % of the number line that is above the range the existing value falls within.
     */
    final Map<Long, Long> startEndRanges;
    final Map<Long, Long> startIdToValueMap;
    final Map<Long, Long> valueToStartIdMap;
    final Long lineLength;

    public Distribution(final Map<Long, Long> startEndRanges, final Map<Long, Long> startIdValue, final Long lineLength) {
        this.startEndRanges = startEndRanges;
        this.startIdToValueMap = startIdValue;
        this.lineLength = lineLength;
        valueToStartIdMap = new HashMap<>();
        for (Map.Entry<Long, Long> entry : startIdValue.entrySet()) {
            valueToStartIdMap.put(entry.getValue(), entry.getKey());
        }
    }

    public static Distribution from(final Map<Long, Long> samples) {
        long lineLength = 0L;
        Map<Long, Long> startEndRanges = new HashMap<>();
        Map<Long, Long> startIdToValue = new HashMap<>();
        for (Map.Entry<Long, Long> entry : samples.entrySet()) {
            startEndRanges.put(lineLength + 1, lineLength + entry.getValue());
            startIdToValue.put(lineLength + 1, entry.getKey());
            lineLength = lineLength + entry.getValue();
        }
        return new Distribution(startEndRanges, startIdToValue, lineLength);
    }

    public double likelihoodToIncreaseCount(final long existingValue) {
        final Long valueRangeStart = valueToStartIdMap.get(existingValue);
        final Long valueRangeEnd = startEndRanges.get(valueRangeStart);
        final long numbersRightHandOfRangeEnd = lineLength - valueRangeEnd;
        return (double) numbersRightHandOfRangeEnd / lineLength;
    }

    public static long findRangeStartIdFromNumberLinePosition(final long numberLinePosition, final Map<Long, Long> startEndRanges) {
        final Iterator<Map.Entry<Long, Long>> i = startEndRanges.entrySet().stream().iterator();
        Map.Entry<Long, Long> last = i.next();
        while (i.hasNext()) {
            if (getRangeStart(last) <= numberLinePosition && numberLinePosition <= getRangeEnd(last)) {
                break;
            }
            last = i.next();
        }
        return getRangeStart(last);
    }

    public static long valueFromRangeStartID(final long rangeStartId, final Map<Long, Long> startIdValueMap) {
        return startIdValueMap.get(rangeStartId);
    }

    public static long getRangeStart(final Map.Entry<Long, Long> entry) {
        return entry.getKey();
    }

    public static long getRangeEnd(final Map.Entry<Long, Long> entry) {
        return entry.getValue();
    }

    // sample a value from the distribution
    public long sample() {
        final long numberLinePosition = (long) (Math.random() * lineLength);
        final long rangeStartId = findRangeStartIdFromNumberLinePosition(numberLinePosition, startEndRanges);
        return valueFromRangeStartID(rangeStartId, startIdToValueMap);
    }
}
