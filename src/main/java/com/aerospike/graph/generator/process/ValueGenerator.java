package com.aerospike.graph.generator.process;

import com.aerospike.graph.generator.emitter.generated.schema.def.ValueGeneratorConfig;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Random;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public abstract class ValueGenerator<T> {
    public static ValueGenerator getGenerator(ValueGeneratorConfig valueGenerator) {
        if(valueGenerator.impl.equals(RandomBoolean.class.getCanonicalName())){
            return RandomBoolean.INSTANCE;
        } else if(valueGenerator.impl.equals(RandomString.class.getCanonicalName())){
            return RandomString.INSTANCE;
        } else if(valueGenerator.impl.equals(RandomDigitSequence.class.getCanonicalName())){
            return RandomDigitSequence.INSTANCE;
        } else {
            try {
                return (ValueGenerator) Class.forName(valueGenerator.impl).getConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
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

}
