package eurocontrol.npp.nm_b2b_showcase;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class Utils {

    /**
     * Concatenate the collection of strings together, using the given separator
     * @param separator
     * @param list
     * @return
     */
    public static String concat(String separator, Collection<String> list) {
        if(list == null) return "";
        StringBuilder sb = new StringBuilder();
        boolean separatorNeeded = false;

        for (String arg : list) {
            if (arg != null) {
                if (separatorNeeded)
                    sb.append(separator);
                else
                    separatorNeeded = true;
                sb.append(arg);
            }
        }
        return sb.toString();
    }

    public static boolean isCompressed(BytesMessage message) {
        try {
            return "gzip".equals(message.getStringProperty("NM_CONTENT_ENCODING"));
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static String getMessageBody(Message message) throws JMSException, IOException {
        if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            return textMessage.getText();

        } else if (message instanceof BytesMessage && Utils.isCompressed((BytesMessage) message)) {
            BytesMessage bytesMessage = (BytesMessage) message;

            long length = bytesMessage.getBodyLength();
            byte[] bytes = new byte[(int) length];
            bytesMessage.readBytes(bytes);

            try (GZIPInputStream gzipIs = new GZIPInputStream(new ByteArrayInputStream(bytes));
                 BufferedReader reader = new BufferedReader(new InputStreamReader(gzipIs))) {
                String messageBody = reader.lines().collect(Collectors.joining());
                return messageBody;
            }
        }
        return null;
    }
}
