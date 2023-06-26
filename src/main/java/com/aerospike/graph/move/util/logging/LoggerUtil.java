//package com.aerospike.graph.move.util.logging;
//
//import org.apache.commons.configuration2.Configuration;
//
//import java.util.Optional;
//
//public class LoggerUtil {
//    private LoggerUtil() {
//    }
//
//    public static LoggerUtil withContext(Class<?> clazz) {
//        return new LoggerUtil(Optional.of(clazz), Optional.empty());
//    }
//
//    public static LoggerUtil withContext(Object object) {
//        return new LoggerUtil(Optional.empty(), Optional.of(object));
//    }
//
//    public static LoggerUtil withContext(Class<?> clazz, Object object) {
//        return (Optional.of(clazz), Optional.of(object));
//    }
//
//    public static abstract class Logger {
//        private final Optional<Class> clazz;
//        private final Optional<Object> object;
//
//        private Logger(final Optional<Class> clazz, final Optional<Object> object) {
//            this.clazz = clazz;
//            this.object = object;
//        }
//        public static Logger create(Configuration config) {
//
//        }
//
//        public abstract void log(final String message);
//
//        public abstract void debug(final String message);
//
//        public abstract void info(final String message);
//
//        public abstract void warn(final String message);
//
//        public abstract void error(final String message);
//
//        public abstract void error(final Throwable message);
//
//    }
//}
