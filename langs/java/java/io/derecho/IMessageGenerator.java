package io.derecho;

import java.nio.ByteBuffer;

/**
 * The interface that users should implement so that they can specify the content of raw messages to
 * be sent through derecho Java.
 */
public interface IMessageGenerator {
  /**
   * Execute the content of this function using the offered [buffer].
   *
   * @param buffer The buffer to be sent to Derecho so that the message stored in this buffer could
   *     be delivered.
   */
  void write(ByteBuffer buffer);
}
