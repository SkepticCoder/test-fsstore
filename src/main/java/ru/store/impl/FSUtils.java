package ru.store.impl;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.file.FileSystems;

public final class FSUtils {

    private static final char EXTENSION_SEPARATOR = '.';
    private static final String DIRECTORY_SEPARATOR = FileSystems.getDefault().getSeparator();

    private FSUtils() {
    }

    public static void close(Channel channel) {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Remove the file extension from a filename, that may include a path.
     *
     * e.g. /path/to/myfile.jpg -> /path/to/myfile
     */
    public static String removeExtension(String filename) {
        if (filename == null) {
            return null;
        }

        int index = indexOfExtension(filename);

        if (index == -1) {
            return filename;
        } else {
            return filename.substring(0, index);
        }
    }

    /**
     * Return the file extension from a filename, including the "."
     *
     * e.g. /path/to/myfile.jpg -> .jpg
     */
    public static String getExtension(String filename) {
        if (filename == null) {
            return null;
        }

        int index = indexOfExtension(filename);

        if (index == -1) {
            return filename;
        } else {
            return filename.substring(index);
        }
    }

    public static int indexOfExtension(String filename) {
        if (filename == null) {
            return -1;
        }

        // Check that no directory separator appears after the
        int extensionPos = filename.lastIndexOf(EXTENSION_SEPARATOR);

        int lastDirSeparator = filename.lastIndexOf(DIRECTORY_SEPARATOR);

        if (lastDirSeparator > extensionPos) {
            return -1;
        }

        return extensionPos;
    }
}
