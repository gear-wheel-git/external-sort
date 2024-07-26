package com.cogwheel.externalsort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;

/**
 *
 * @author gear-wheel
 * @date 2024-07-26 9:43
 */
@SuppressWarnings("all")
class ExternalSortUtils {

    private static final Logger log = LoggerFactory.getLogger(ExternalSortUtils.class);

    private static final Object unsafe;
    private static final Method cleanerMethod;
    private static final Method cleanMethod;
    private static final Method invokeCleanerMethod;

    private static final String[] UNIT_NAMES = new String[]{"B", "KB", "MB", "GB", "TB", "PB", "EB"};


    static {
        ByteBuffer tempBuffer = ByteBuffer.allocateDirect(0);
        Method cleanerMethodLocal = null;
        Method cleanMethodLocal = null;
        Object unsafeLocal = null;
        Method invokeCleanerMethodLocal = null;
        if (getJvmVersion() >= 9) {
            try {
                Class<?> clazz = Class.forName("sun.misc.Unsafe");
                Field theUnsafe = clazz.getDeclaredField("theUnsafe");
                theUnsafe.setAccessible(true);
                unsafeLocal = theUnsafe.get(null);
                invokeCleanerMethodLocal = clazz.getMethod("invokeCleaner", ByteBuffer.class);
                invokeCleanerMethodLocal.invoke(unsafeLocal, tempBuffer);
            } catch (IllegalAccessException | IllegalArgumentException
                     | InvocationTargetException | NoSuchMethodException | SecurityException
                     | ClassNotFoundException | NoSuchFieldException e) {
                log.warn("byteBufferUtils.cleaner", e);
                unsafeLocal = null;
                invokeCleanerMethodLocal = null;
            }
        } else {
            try {
                cleanerMethodLocal = tempBuffer.getClass().getMethod("cleaner");
                cleanerMethodLocal.setAccessible(true);
                Object cleanerObject = cleanerMethodLocal.invoke(tempBuffer);
                cleanMethodLocal = cleanerObject.getClass().getMethod("clean");
                cleanMethodLocal.invoke(cleanerObject);
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException |
                     IllegalArgumentException | InvocationTargetException e) {
                log.warn(e.getLocalizedMessage(), e);
                cleanerMethodLocal = null;
                cleanMethodLocal = null;
            }
        }
        cleanerMethod = cleanerMethodLocal;
        cleanMethod = cleanMethodLocal;
        unsafe = unsafeLocal;
        invokeCleanerMethod = invokeCleanerMethodLocal;
    }

    private ExternalSortUtils() {}


    public static void cleanDirectBuffer(ByteBuffer buf) {
        if (cleanMethod != null) {
            try {
                cleanMethod.invoke(cleanerMethod.invoke(buf));
            } catch (IllegalAccessException | IllegalArgumentException
                     | InvocationTargetException | SecurityException e) {
                if (log.isDebugEnabled()) {
                    log.debug(e.getLocalizedMessage(), e);
                }
            }
        } else if (invokeCleanerMethod != null) {
            try {
                invokeCleanerMethod.invoke(unsafe, buf);
            } catch (IllegalAccessException | IllegalArgumentException
                     | InvocationTargetException | SecurityException e) {
                if (log.isDebugEnabled()) {
                    log.debug(e.getLocalizedMessage(), e);
                }
            }
        }
    }


    private static int getJvmVersion() {
        int jvmVersion = -1;

        try{
            String javaSpecVer = System.getProperty("java.specification.version");
            if (!"".equals(javaSpecVer)) {
                if (javaSpecVer.startsWith("1.")) {
                    javaSpecVer = javaSpecVer.substring(2);
                }
                if (javaSpecVer.indexOf('.') == -1) {
                    jvmVersion = Integer.parseInt(javaSpecVer);
                }
            }
        } catch (Throwable ignore){
            jvmVersion = 8;
        }

        return jvmVersion;
    }


    public static String format(long size) {
        if (size <= 0) {
            return "0";
        }
        int digitGroups = Math.min(UNIT_NAMES.length-1, (int) (Math.log10(size) / Math.log10(1024)));
        return new DecimalFormat("#,##0.##")
                .format(size / Math.pow(1024, digitGroups)) + " " + UNIT_NAMES[digitGroups];
    }
}
