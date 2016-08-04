package info.fetter.logstashforwarder.util;

import info.fetter.logstashforwarder.FileWatcher;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.apache.log4j.Logger;

public class BufferedRandomAccessFileReadOnly extends RandomAccessFile {

    private static final Logger logger = Logger.getLogger(FileWatcher.class);

    private byte buffer[];
    private int bufferSize = 0;

    private long filePos = 0;
    private long fileLength = 0;
    private long bufferStart = 0;

    public BufferedRandomAccessFileReadOnly(String filename, String mode, int bufsize) throws IOException {
        this(new File(filename), mode, bufsize);
        logger.info("new BufferedRandomAccessFileReadOnly");
    }

    public BufferedRandomAccessFileReadOnly(File file, String mode, int bufsize) throws IOException {
        super(file, mode);
        fileLength = super.length();
        buffer = new byte[bufsize];
        logger.info("new BufferedRandomAccessFileReadOnly");
    }

    @Override
    public final int read() throws IOException {
        while (true) {
            if (filePos == fileLength) {
                fileLength = super.length();
                updateReadBuffer();
            }
            if (filePos == fileLength) {
                return -1;
            }
            // read the data
            int readAtIdx = (int) (filePos - bufferStart);
            if (readAtIdx < 0 || readAtIdx >= bufferSize) {
                fileLength = super.length();
                updateReadBuffer();
            } else {
                ++filePos;
                return ((int) buffer[readAtIdx]) & 0xff;
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        logger.info("batch read;off:" + off + ";len:" + len);
        int fileAvailable = (int) (super.length() - filePos);
        if (fileAvailable == 0) {
            return -1;
        }
        if (len > fileAvailable) {
            len = fileAvailable;
        }
        int readAtIdx = (int) (filePos - bufferStart);
        if (readAtIdx < 0 || readAtIdx >= bufferSize) {
            updateReadBuffer();
            readAtIdx = (int) (filePos - bufferStart);
        }
        int availableInBuffer = bufferSize - readAtIdx;
        if (len > availableInBuffer) {
            len = availableInBuffer;
        }
        System.arraycopy(buffer, readAtIdx, b, off, len);
        filePos += len;
        return len;
    }

    @Override
    public void write(int b) throws IOException {
        throw new IOException("Read only implementation!");
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        throw new IOException("Read only implementation!");
    }

    private void updateReadBuffer() throws IOException {
        super.seek(filePos);
        bufferStart = filePos;
        int n = super.read(buffer, 0, buffer.length);
        if (n < 0) {
            n = 0;
        }
//        logger.info("Read performed for " + n + " bytes");
        bufferSize = n;
    }

    @Override
    public long getFilePointer() throws IOException {
        return filePos;
    }

    @Override
    public void seek(long pos) throws IOException {
//        logger.info("SEEK! filePos:" + filePos + ";newPos:" + pos + ";fileLength:" + fileLength);
        filePos = pos;
        if (filePos > fileLength) {
            filePos = fileLength;
        }
        if (filePos < 0) {
            filePos = 0;
        }
    }

    @Override
    public void setLength(long newLength) throws IOException {
        throw new IOException("Read only implementation!");
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}

