package com.aerospike.graph.generator.process;

import com.aerospike.graph.generator.emitter.generated.schema.def.ValueGeneratorConfig;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public abstract class ValueGenerator<T> {
    public static ValueGenerator getGenerator(ValueGeneratorConfig valueGenerator) {
        if (valueGenerator.impl.equals(RandomBoolean.class.getCanonicalName())) {
            return RandomBoolean.INSTANCE;
        } else if (valueGenerator.impl.equals(RandomString.class.getCanonicalName())) {
            return RandomString.INSTANCE;
        } else if (valueGenerator.impl.equals(RandomDigitSequence.class.getCanonicalName())) {
            return RandomDigitSequence.INSTANCE;
        } else if (valueGenerator.impl.equals(FormattedRandomSSN.class.getCanonicalName())) {
            return FormattedRandomSSN.INSTANCE;
        }  else if (valueGenerator.impl.equals(FormattedRandomUSAddress.class.getCanonicalName())) {
            return FormattedRandomUSAddress.INSTANCE;
        } else if (valueGenerator.impl.equals(FormattedRandomUSPhone.class.getCanonicalName())) {
            return FormattedRandomUSPhone.INSTANCE;
        } else if (valueGenerator.impl.equals(FormattedRandomUSZip.class.getCanonicalName())) {
            return FormattedRandomUSZip.INSTANCE;
        } else {
            try {
                return (ValueGenerator) Class.forName(valueGenerator.impl).getConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                     NoSuchMethodException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    abstract public T generate(Map<String, Object> params);

    public static class RandomBoolean extends ValueGenerator<Boolean> {
        public static RandomBoolean INSTANCE = new RandomBoolean();

        @Override
        public Boolean generate(final Map<String, Object> params) {
            return new Random().nextBoolean();
        }
    }

    public static class RandomString extends ValueGenerator<String> {
        public static RandomString INSTANCE = new RandomString();

        private String randomString(int len) {
            final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
            final Random rnd = new Random();
            final StringBuilder sb = new StringBuilder(len);
            for (int i = 0; i < len; i++)
                sb.append(AB.charAt(rnd.nextInt(AB.length())));
            return sb.toString();
        }

        public String generate(Map<String, Object> params) {
            return randomString((Integer) params.get("length"));
        }
    }

    public static class RandomDigitSequence extends ValueGenerator<Long> {
        public static RandomDigitSequence INSTANCE = new RandomDigitSequence();

        @Override
        public Long generate(final Map<String, Object> params) {
            int digits = (Integer) params.get("digits");
            StringBuilder sb = new StringBuilder(digits);
            for (int i = 0; i < digits; i++) {
                sb.append(new Random().nextInt(10));
            }
            return Long.valueOf(sb.toString());
        }
    }

    public static class FormattedRandomSSN extends ValueGenerator<String> {
        public static FormattedRandomSSN INSTANCE = new FormattedRandomSSN();

        public String generate(Map<String, Object> params) {
            return String.format("%03d-%02d-%04d",
                    new Random().nextInt(1000),
                    new Random().nextInt(100),
                    new Random().nextInt(10000));
        }
    }

    public static class FormattedRandomUSPhone extends ValueGenerator<String> {
        public static FormattedRandomUSPhone INSTANCE = new FormattedRandomUSPhone();

        public String generate(Map<String, Object> params) {
            return String.format("+1 (%03d) %03d-%04d",
                    new Random().nextInt(1000),
                    new Random().nextInt(1000),
                    new Random().nextInt(10000));
        }
    }

    public static class FormattedRandomUSZip extends ValueGenerator<String> {
        public static FormattedRandomUSZip INSTANCE = new FormattedRandomUSZip();

        public String generate(Map<String, Object> params) {
            return String.format("%05d",
                    new Random().nextInt(100000));
        }
    }

    public static class FormattedRandomUSAddress extends ValueGenerator<String> {
        private Set<String> streetNames = Set.of(
                "Maple", "Oak", "Pine", "Elm", "Cedar", "Willow", "Birch", "Juniper", "Ash", "Cypress", "Magnolia", "Spruce", "Hawthorn", "Aspen", "Sycamore", "Linden", "Poplar", "Chestnut", "Mulberry", "Redwood", "Dogwood", "Alder", "Cherry", "Walnut", "Beech", "Hazel", "Locust", "Yew", "Hemlock", "Sequoia", "Ginkgo", "Olive", "Acacia", "Baobab", "Eucalyptus", "Mimosa", "Sassafras", "Cactus", "Fig", "Honeysuckle", "Jacaranda", "Kiwi", "Myrtle", "Nectarine", "Pecan", "Plum", "Quince", "Raspberry", "Sage", "Tamarind", "Verbena", "Wisteria", "Xylosma", "Yam", "Zinnia", "Begonia", "Calendula", "Dahlia", "Echinacea");
        private Set<String> streetSuffix = Set.of("ln", "ct", "st", "cir", "sq", "dr", "ave", "rd", "blvd", "way", "pl", "ter", "trl", "pkwy", "hwy");
        public static FormattedRandomUSAddress INSTANCE = new FormattedRandomUSAddress();

        public String generate(Map<String, Object> params) {
            return String.format("%d %s %s",
                    new Random().nextInt(10000),
                    streetNames.stream().skip(new Random().nextInt(streetNames.size())).findFirst().get(),
                    streetSuffix.stream().skip(new Random().nextInt(streetSuffix.size())).findFirst().get());
        }
    }
}
